import logging
import ibm_db
import pandas as pd
import os
import pickle
from configuration import Config
from dao import get_db2_connect


def recursive_expand_fk(left_node, unexpand_fk_dict):
    """
    找出全部的外键关系，即每个节点通过外键关系可以推出的全部节点
    该方法是递归调用，即如果有C字段FK关系推出D，那么也要把D字段FK关系追加到A字段上
    A字段FK关系推出B字段，B字段FK关系推出C字段
    A字段 --> left_node
    B字段列表 --> unexpand_fk_dict[left_node]
    C字段列表 --> unexpand_fk_dict[fk]
    :param left_node: 外键关系左部
    :param unexpand_fk_dict: 未经扩展的全部外键关系字典
    :return:
    """
    # 用于保存所有的C字段的列表
    extend_fk = []
    # unexpand_fk_dict[left_node]为外键关系右部列表
    for fk in unexpand_fk_dict[left_node]:
        # 如果外键关系右部在其他的外键关系中充当左部
        if fk in unexpand_fk_dict.keys():
            # 即A字段FK关系推出B字段，B字段FK关系推出C字段，则将C字段追加到extend_fk列表中
            extend_fk.extend(unexpand_fk_dict[fk])
    fk_list = unexpand_fk_dict[left_node]
    fk_list_new = fk_list.copy()
    # B字段和C字段的列表
    fk_list_new.extend(extend_fk)
    # 去重之后的B字段和C字段的列表
    fk_list_new = list(set(fk_list_new))
    # len(fk_list_new) == len(fk_list)，表示不存在C字段，只有A字段推B字段
    if len(fk_list_new) == len(fk_list):
        return fk_list_new
    # 直到找到每个节点通过外键关系可以推出的全部节点
    else:
        unexpand_fk_dict[left_node] = fk_list_new
        return recursive_expand_fk(left_node, unexpand_fk_dict)


def expand_fk_relation(unexpand_fk_dict):
    """
    对unexpand_fk_dict进行处理，找出每个左部节点的全部的外键关系，即每个节点通过外键关系可以推出的全部节点
    :param unexpand_fk_dict: 未经扩展的原始外键关系
    :return: 经过扩展的外键关系
    """
    for key in unexpand_fk_dict.keys():
        fk_list = recursive_expand_fk(key, unexpand_fk_dict)
        unexpand_fk_dict[key] = fk_list
    return unexpand_fk_dict


def get_derive_from_each_other_fk(expand_fk_dict):
    """
    通过外键关系，找出可以互推的节点集合，即A和B互为外键，在实际业务中表现为A，B所在表分属不同的系统，且B从A中同步数据
    A字段和B字段做业务主键
    :param expand_fk_dict: 经过扩展的外键关系字典
    :return: res_dict:{"A":"A/B", "B":"A/B"}
    """
    # 用于存放外键关系中能互推的所有节点集合:[[A,B],[C,D]]
    dfeos = []
    # 用于总结所有可以互推的节点，比如在遍历A节点找A节点能互推的节点时，如果找到B节点，就把B节点加入skip_key，这样在遍历B节点可以互推的节点时，就可以跳过A节点
    skip_key = []
    for key, value in expand_fk_dict.items():
        tmp_list = [key]
        if key not in skip_key:
            # 由于上一步对外键关系进行了扩展，可以知道，只有value中存在key这种情况的key才存在互推关系，比如A,B互推，则A:[B,C,A], B:[A,C,B]
            if key in value:
                skip_key.append(key)
                for vl in value:
                    # 找vl能推出的字段
                    if vl in expand_fk_dict.keys():
                        # 如果vl能推出key，并且不是A推出A这种情况
                        if key in expand_fk_dict[vl] and vl != key:
                            # [A,B]
                            tmp_list.append(vl)
                            skip_key.append(vl)
            tmp_list = list(set(tmp_list))
            dfeos.append(tmp_list)
    res_dict = {}
    for dfeo in dfeos:
        dfeo.sort()
        if len(dfeo) > 1:
            # A/B
            new_node = '/'.join(dfeo)
            for node in dfeo:
                res_dict[node] = new_node
    # res_dict:{"A":"A/B", "B":"A/B"}
    return res_dict


def find_candidate_dim_node(fk_df):
    """
    初步找出维度主节点，原则是：外键关系只出不进的认为是维度主节点
    :param fk_df: 完成外键互推关系合并，并删除指向自身关系的外键DataFrame
    :return: 候选维度主节点列表
    """
    # 初步找出维度主节点，外键关系只出不进的节点
    left_list = fk_df['LEFT'].tolist()
    # 给外键关系左部去重
    left_list_unq = list(set(left_list))
    # 外键关系右部
    right_list = fk_df['RIGHT'].tolist()
    candidate_dim_node = []
    for i in left_list_unq:
        # 节点i只存在外键关系（出），不存在外键关系（入），此时可认为是维度主节点（初步判断）
        if i not in right_list:
            candidate_dim_node.append(i)
    return candidate_dim_node


def find_attr_and_subset(candidate_dim_node, after_arrange_fd_dict, after_arrange_fk_dict):
    """
    找出维度节点root的全部属性节点和子集节点
    :param candidate_dim_node:候选维度主节点
    :param after_arrange_fd_dict: 整理好的fd关系
    :param after_arrange_fk_dict: 整理好的fk关系
    :return:attr_list->属性节点列表, subset_list->子集节点列表
    """
    node_dict = {'attr': set(), 'subset': {candidate_dim_node}}
    # candidate_dim_node节点的全部子节点
    node_dict_res = find_subset(after_arrange_fd_dict, after_arrange_fk_dict, node_dict)
    # 属性集合添加维度主节点自身，去重，将属性集合和子集集合均转换为列表，返回
    attr_res = list(node_dict_res['attr'])
    # 属性集合中添加候选维度主节点自身
    attr_res.append(candidate_dim_node)
    attr_list = list(set(attr_res))
    subset_list = list(node_dict_res['subset'])
    return attr_list, subset_list


def find_subset(after_arrange_fd_dict, after_arrange_fk_dict, node_dict):
    """
    找出维度节点root的全部属性节点和子集节点
    :param after_arrange_fd_dict: 整理好的fd关系
    :param after_arrange_fk_dict: 整理好的fk关系
    :param node_dict: node_dict = {'attr': set(), 'subset': {root}},之所以将本身放在子集集合而不是放在属性集合里面，
            就是因为如果rootFK关系推出的节点应该放在子集集合中，如果将root放在属性集合里面，那么rootFK关系推出的节点就会被放到属性集合里面，这样就出错了。
    :return:
    整个程序的总体逻辑就是，先找到候选维度主节点(root)能够推出的属性集合和子集集合，然后再遍历推出的属性集合和子集集合，
    属性FD推出的节点还是root的属性，属性FK推出的节点是root的属性，
    子集FK推出的节点还是root的子集，子集FD推出的节点是root的属性，
    一直循环去寻找，直到找不到为止，则这个维度主节点的属性集合和子集集合就被全部找到了，可以返回
    """
    flag = True
    while flag:
        # root节点属性的属性是root节点的属性
        attr_from_attr = set()
        # root节点子集的子集是root节点的子集
        subset_from_subset = set()
        # root节点子集的属性是root节点的属性
        attr_from_subset = set()
        for attr in node_dict['attr']:
            # 如果root节点的属性既做FD关系的左部，又做FK关系的左部，则属性FD推出的节点还是root的属性，属性FK推出的节点是root的属性
            if attr in after_arrange_fd_dict.keys() and attr in after_arrange_fk_dict.keys():
                attr_from_attr = attr_from_attr | after_arrange_fd_dict[attr] | after_arrange_fk_dict[attr]
            # 如果root节点的属性只做FD关系的左部，则属性FD推出的节点还是root的属性
            elif attr in after_arrange_fd_dict.keys() and attr not in after_arrange_fk_dict.keys():
                attr_from_attr = attr_from_attr | after_arrange_fd_dict[attr]
            # 如果root节点的属性只做FK关系的左部，属性FK推出的节点是root的属性
            elif attr not in after_arrange_fd_dict.keys() and attr in after_arrange_fk_dict.keys():
                attr_from_attr = attr_from_attr | after_arrange_fk_dict[attr]
        for subset in node_dict['subset']:
            # 如果某子集节点在FD关系的左部，则右部作为属性保存到root节点中
            if subset in after_arrange_fd_dict.keys():
                attr_from_subset = attr_from_subset | after_arrange_fd_dict[subset]
            # 如果某子集节点在FK关系的左部，则右部作为子集保存到root节点中
            if subset in after_arrange_fk_dict.keys():
                subset_from_subset = subset_from_subset | after_arrange_fk_dict[subset]
        # 合并属性节点
        # attr_add和subset_add为本次while循环新找出来的属性和子集
        attr_add = attr_from_attr | attr_from_subset
        subset_add = subset_from_subset
        new_attr = node_dict['attr'] | attr_add
        new_subset = node_dict['subset'] | subset_add
        # 当本次和上次的属性集合和子集集合长度不再发生变化的时候，控制flag跳出循环
        if len(new_attr) == len(node_dict['attr']) and len(new_subset) == len(node_dict['subset']):
            flag = False
        else:
            node_dict['attr'] = new_attr
            node_dict['subset'] = new_subset
    # 如果节点为该维度的属性，则不能为该维度的子集
    # 找出既作为该节点的属性，又作为该节点的子集的部分，去掉子集节点，保留属性节点
    subset_del = node_dict['attr'] & node_dict['subset']
    subset_new = node_dict['subset'] - subset_del
    node_dict['subset'] = subset_new
    # 返回
    return node_dict


def dim_main_node_check(candidate_dim_node_list, all_attr_rela_list, all_dim_node_rela):
    """
    检查初步判定的维度节点是否可作为维度主节点
    :param candidate_dim_node_list: 初步判定的维度主节点列表
    :param all_attr_rela_list: 所有候选维度主节点属性列表[(dim_node, attr1),(dim_node, attr2),...]
    :param all_dim_node_rela: 所有候选维度主节点的属性关系和子集关系
    :return:
    """
    #
    # dim_main_node_check_res = {}
    # for candidate_dim_node in candidate_dim_node_list:
    #     # 某维度节点做其他维度节点属性的个数，大于等于2或等于0时该节点可作为一个维度节点，即当一个维度主节点不和其他任何维度主节点产生关系，或其本身又作为其他维度主节点的属性数目大于等于2时
    #     node_attr_list = []
    #     # 遍历属性列表
    #     for attr in all_attr_rela_list:
    #         # subsets是维度节点和他的子集，attr_list是全部的属性关系。node_attr_list是维度节点所属的属性列表
    #         # 下面就是判断当前维度节点的是否在其他维度节点的属性中
    #         if attr[1] == candidate_dim_node and attr[0] not in node_attr_list and attr[0] != candidate_dim_node:
    #             node_attr_list.append(attr[0])
    #         # check_res[node] = True表示该节点可以作为维度节点
    #         if len(node_attr_list) >= 2:
    #             dim_main_node_check_res[candidate_dim_node] = True
    #             break
    #     if len(node_attr_list) == 0:
    #         dim_main_node_check_res[candidate_dim_node] = True
    #     if len(node_attr_list) == 1:
    #         dim_main_node_check_res[candidate_dim_node] = node_attr_list[0]
    # return dim_main_node_check_res
    #
    # 检查节点是否可作为维度节点
    # 先找出最顶级的根维度节点，这种维度节点不是其他任何维度的属性（排除同表的）
    # all_attr.append((dim_node, attr))
    group_dict = group_by_same_tab(candidate_dim_node_list)
    # key为候选维度主节点名，value为分组号
    group_num_dict = get_group_num(group_dict)
    # 存放维度主节点的列表
    root_dim_list = []
    node_list = list(set(candidate_dim_node_list))
    # 先遍历一遍候选维度节点，只要该维度节点做其他维度节点的属性，则认为该节点不是维度主节点，先找出一批不和其他维度节点产生关系的候选维度节点作为维度主节点
    for node in node_list:
        root_flag = True
        for attr in all_attr_rela_list:
            # 如果候选维度节点做属性
            if node == attr[1]:
                # 并且候选维度节点分组号不等于它的维度主节点分组号
                if group_num_dict[node] != group_num_dict[attr[0]]:
                    # node所属表和其作为属性所属的维度节点所属表不同，说明node不能作为根维度节点
                    root_flag = False
                    break
        if root_flag:
            root_dim_list.append(node)
            all_dim_node_rela[node] = True
    node_list = list(set(node_list) ^ set(root_dim_list))
    # 遍历其余维度节点，若该维度节点是两个以上根维度节点的属性（若这两个根维度节点同表，认为是一个），则认为该维度节点可作为维度主节点
    for node in node_list:
        node_attr_dim = []
        for attr in all_attr_rela_list:
            if node == attr[1] and attr[0] in root_dim_list and attr[0] != node:
                if len(node_attr_dim) == 0:
                    node_attr_dim.append(attr[0])
                elif group_num_dict[attr[0]] != group_num_dict[node_attr_dim[0]]:
                    node_attr_dim.append(attr[0])
                if len(node_attr_dim) >= 2:
                    all_dim_node_rela[node] = True
                    break
        if len(node_attr_dim) == 1:
            all_dim_node_rela[node] = node_attr_dim[0]
        if len(node_attr_dim) == 0:
            all_dim_node_rela[node] = True
    return all_dim_node_rela, group_dict, group_num_dict


def candidate_node_find_main_node(dim_main_node_check_res):
    """
    对于无法做维度主节点的节点，找到其对应的维度主节点
    :param dim_main_node_check_res: 经过校验之后的维度主节点字典
    :return:
    """
    check_alter = dim_main_node_check_res
    for dim_left, dim_right in dim_main_node_check_res.items():
        if dim_right is not True:
            check_alter[dim_left] = attr_trans(dim_left, dim_main_node_check_res, dim_left)
    return check_alter


def attr_trans(left_dim_node, dim_main_node_check_res, node_orig):
    """
    将非维度主节点找到其所属的维度主节点
    :param left_dim_node: 不能做维度主节点的候选维度主节点
    :param dim_main_node_check_res: 经过校验之后的维度主节点字典
    :param node_orig
    :return:
    """
    #
    # if dim_main_node_check_res[left_dim_node] in dim_main_node_check_res.keys():
    #     if dim_main_node_check_res[left_dim_node] is True \
    #             or dim_main_node_check_res[dim_main_node_check_res[left_dim_node]] is True:
    #         return dim_main_node_check_res[left_dim_node]
    #     # A的维度主节点是B，B的维度主节点是A，则合并两个维度节点当做一个维度主节点
    #     elif dim_main_node_check_res[dim_main_node_check_res[left_dim_node]] == left_dim_node:
    #         tab1 = left_dim_node.split('|')[1]
    #         tab2 = dim_main_node_check_res[left_dim_node].split('|')[1]
    #         if tab1 == tab2:
    #             return left_dim_node
    #         return left_dim_node
    #     # 当前节点的维度节点还不是维度主节点，所以要继续找，直到返回一个维度主节点
    #     else:
    #         return attr_trans(dim_main_node_check_res[left_dim_node], dim_main_node_check_res)
    # else:
    #     return dim_main_node_check_res[left_dim_node]
    #
    # 将非维度节点找到其所属的维度节点
    if dim_main_node_check_res[left_dim_node] is True:
        # if attr_dict[node] is True or attr_dict[attr_dict[node]] is True:
        return dim_main_node_check_res[left_dim_node]
    elif dim_main_node_check_res[left_dim_node] == node_orig:
        return True
    else:
        return attr_trans(dim_main_node_check_res[left_dim_node], dim_main_node_check_res, node_orig)


def same_tab_dim_node_merge(candidate_node_find_main_node_res, group_dict, group_num_dict):
    """
    将同一张表内的维度节点合并，例如维度节点tab01_aa,tab01_bb => tab01_aa/tab01_bb
    :param candidate_node_find_main_node_res: 维度节点之间的关系字典，{节点1：节点2, ...},key为初步判定的维度节点名，
            value为True表示该节点为维度主节点，为其他值表示该key不是维度主节点，value是其维度主节点
    :param group_dict
    :param group_num_dict
    :return:
    """
    for dim in candidate_node_find_main_node_res.keys():
        if candidate_node_find_main_node_res[dim] is True:
            group_num = group_num_dict[dim]
            dim_list = list(group_dict[group_num]['dim'])
            if len(dim_list) > 1:
                dim_list.sort()
                candidate_node_find_main_node_res[dim] = '/'.join(dim_list)
    return candidate_node_find_main_node_res
    #
    # # 提取维度节点所属的表名
    # dim_tab_dict = {}
    # # 只合并同表维度主节点
    # for dim in candidate_node_find_main_node_res.keys():
    #     dim_nodes = dim.split('/')
    #     if len(dim_nodes) == 1 and candidate_node_find_main_node_res[dim] is True:
    #         dim_node = dim_nodes[0]
    #         tab = dim_node.split('|')[1]
    #         if tab not in dim_tab_dict.keys():
    #             dim_tab_dict[tab] = []
    #             dim_tab_dict[tab].append(dim)
    #         else:
    #             dim_tab_dict[tab].append(dim)
    #
    # # value为属于某个表的维度节点列表，当其长度大于1时说明可能需要合并其中的维度节点（其中可以fd互推的节点可以合并）创建新的维度
    # # t同一张表中的fd互推的维度节点合并为一个维度
    # for key, value in dim_tab_dict.items():
    #     if len(value) > 1:
    #         group_list = []
    #         value_alr_arrange = []
    #         for i in range(len(value)):
    #             if value[i] in value_alr_arrange:
    #                 continue
    #             group = [value[i]]
    #             for j in range(len(value)):
    #                 if i == j:
    #                     continue
    #                 if value[j] in ori_fd_dict[value[i]] and value[i] in ori_fd_dict[value[j]]:  # fd互推
    #                     group.append(value[j])
    #                     value_alr_arrange.append(value[j])
    #             group = list(set(group))
    #             group_list.append(group)
    #         for group in group_list:
    #             if len(group) > 1:
    #                 new_dim = '/'.join(value)
    #                 new_dim = '#' + new_dim
    #                 # dim_dict[A1] = #A1/A2
    #                 # dim_dict[A2] = #A1/A2
    #                 for i in group:
    #                     candidate_node_find_main_node_res[i] = new_dim
    # return candidate_node_find_main_node_res
    #


def node_arrange(orig_dict):
    """
    对得到的维度划分结果进行整理，方便保存进入数据库
    :param orig_dict: 得到的维度划分结果
    :return: 整理后的维度划分结果
    """
    res_dict = {'sys': [], 'tab': [], 'node': [], 'dim': [], 'orig_dim': [], 'type': [], 'del_flag': []}
    for i in range(len(orig_dict['node'])):
        nodes = orig_dict['node'][i]
        node_list = nodes.split('/')
        if len(node_list) == 1:
            node = node_list[0]
            node_info_list = node.split('|')
            res_dict['sys'].append(node_info_list[0])
            res_dict['tab'].append(node_info_list[1])
            res_dict['node'].append(node_info_list[2])
            res_dict['dim'].append(orig_dict['dim'][i])
            res_dict['orig_dim'].append(orig_dict['orig_dim'][i])
            res_dict['type'].append(orig_dict['type'][i])
            res_dict['del_flag'].append(orig_dict['del_flag'][i])
        else:
            for node in node_list:
                node_info_list = node.split('|')
                res_dict['sys'].append(node_info_list[0])
                res_dict['tab'].append(node_info_list[1])
                res_dict['node'].append(node_info_list[2])
                res_dict['dim'].append(orig_dict['dim'][i])
                res_dict['orig_dim'].append(orig_dict['orig_dim'][i])
                res_dict['type'].append(orig_dict['type'][i])
                res_dict['del_flag'].append(orig_dict['del_flag'][i])
    return res_dict


def del_temp_file():
    if os.path.exists('../tmp/after_merge_fk_df.csv'):
        os.remove('../tmp/after_merge_fk_df.csv')
    if os.path.exists('../tmp/after_merge_all_rela_df.csv'):
        os.remove('../tmp/after_merge_all_rela_df.csv')
    if os.path.exists('../tmp/candidate_dim_node_list.pickle'):
        os.remove('../tmp/candidate_dim_node_list.pickle')
    if os.path.exists('../tmp/all_attr_rela.pickle'):
        os.remove('../tmp/all_attr_rela.pickle')
    if os.path.exists('../tmp/all_subset_rela.pickle'):
        os.remove('../tmp/all_subset_rela.pickle')
    if os.path.exists('../tmp/all_dim_node_rela.pickle'):
        os.remove('../tmp/all_dim_node_rela.pickle')

    logging.info('本地文件已删除')


def group_by_same_tab(dim_list):
    """
    同一表的候选维度主节点分为一组，包括特殊情况A/Bfk互推,B/Cfk互推,C/Dfk互推,这三个维度主节点也认为是同表
    :param dim_list:
    :return: group_dict{'1':{dim:(a1,b1,c1),tab:(a,b,c)}, '2':{}....}
    """
    group_dict = {}
    group_num = 0
    dim_need_alloc_set = set(dim_list)
    while True:
        if len(dim_need_alloc_set) == 0:
            break
        group_num += 1
        dim = list(dim_need_alloc_set)[0]
        group_dict[str(group_num)] = {}
        group_dict[str(group_num)]['dim'] = {dim}
        # 如果维度主节点是经过fk关系合并的，就会有/做分隔，截取之后，再使用|做分隔，因为节点名是系统名|表名|字段名
        group_dict[str(group_num)]['tab'] = set([d.split('|')[1] for d in dim.split('/')])
        # 取对称差集
        dim_need_alloc_set = dim_need_alloc_set ^ {dim}
        group_need_alloc_flag = True
        while group_need_alloc_flag:
            group_need_alloc_flag = False
            dim_alloced_list = []
            for node in list(dim_need_alloc_set):
                # 所有候选维度主节点所在表名
                node_tab_set = set([n.split('|')[1] for n in node.split('/')])
                if len(node_tab_set & group_dict[str(group_num)]['tab']) > 0:
                    # 有重叠可认为是同一个表
                    group_dict[str(group_num)]['dim'] = group_dict[str(group_num)]['dim'] | {node}
                    group_dict[str(group_num)]['tab'] = group_dict[str(group_num)]['tab'] | node_tab_set
                    dim_alloced_list.append(node)
                    group_need_alloc_flag = True
            dim_need_alloc_set = dim_need_alloc_set ^ set(dim_alloced_list)
    return group_dict


def get_group_num(group_dict):
    """
    获取所有dim所属的group号
    :param group_dict:
    :return: group_num_dict: {dim1:'1', dim2: '2',....}
    """
    group_num_dict = {}
    for group_num in group_dict:
        for dim in list(group_dict[group_num]['dim']):
            if dim not in group_num_dict.keys():
                group_num_dict[dim] = group_num
            else:
                print('同一dim分到多个分组，分组错误！')
    return group_num_dict


def dim_division(conf):
    """
    维度划分逻辑处理
    1、在数据库中读取数据函数依赖关系和外键关系，目前暂时只取FD_LEVEL = 1的函数依赖关系和单一外键关系
    2、将两个结果集转换为pandas的DataFrame
    3、对数据进行初步处理，主要是删除重复项
    4、去重之后的FD关系和FK关系进行开始进行维度划分
        4-1、处理FK关系，对FK关系进行扩展，可以进行FK关系互推的将其合并为一个节点
        4-2、删除fk和fd关系中，指向自身的关系，如
               LEFT          RIGHT           RL
         0     A/B           A/B             FK
         1     A/B           A/B             FK
        都会被删掉
    5、初步找出维度主节点，原则是：外键关系只出不进的认为是维度主节点
    6、整理fd和fk关系
    7、遍历初步判定的维度主节点，找出每个维度节点的属性列表和子集列表，属性列表指的是FD关系，子集列表指的是FK关系
    8、检查初步判定的维度节点之间的关系，检查初步判定的维度节点是否可作为维度主节点，原则是如果一个维度节点不做任何节点的属性，或者做两个以上节点的属性，则该节点认定为维度主节点
    9、对无法作为维度主节点的节点，找到其对应的维度主节点
    10、对同表中可以互推的维度主节点进行合并
    11、整理所有节点所属的维度
    12、保存维度划分结果
    :param conf:
    :return:
    """
    assert isinstance(conf, Config)

    output_conn = None
    output_helper = None
    if conf.output_db == "db2":
        import dao.output.db2_helper as output_helper
        output_conn = get_db2_connect(conf.output_db_url)
    else:
        logging.error("输出配置数据库未适配:{}".format(conf.output_db))
        exit(-1)

    logging.info('开始删除旧的维度划分结果')
    del_result_code = output_helper.del_old_dim_dive_result(output_conn, conf.output_schema)
    if del_result_code == 0:
        logging.info('删除旧的维度划分结果完成')
    elif del_result_code == -1:
        logging.error('删除旧的维度划分结果失败')
        ibm_db.close(output_conn)
        exit(-1)
    else:
        logging.error('删除旧的维度划分结果返回未知的状态码{}'.format(del_result_code))
        ibm_db.close(output_conn)
        exit(-1)

    # 1、在数据库中读取数据函数依赖关系和外键关系，目前暂时只取FD_LEVEL = 1的函数依赖关系和单一外键关系
    logging.info('开始读取数据')
    # FD_LEVEL = 1的函数依赖关系
    fd_dict_from_db = output_helper.get_function_dependency(output_conn, conf.output_schema)
    # 单一外键关系
    fk_dict_from_db = output_helper.get_single_fk_relation(output_conn, conf.output_schema)

    # FIXME 从ANAschema中拿到函数依赖关系和外键关系，用来校验程序，后续可以删掉
    # fd_dict_from_db = output_helper.get_fd_for_dim_dive(output_conn, "ANA")
    # fk_dict_from_db = output_helper.get_fk_for_dim_dive(output_conn, "ANA")

    # 2、将两个结果集转换为pandas的DataFrame
    fd_df = pd.DataFrame(fd_dict_from_db)
    fk_df = pd.DataFrame(fk_dict_from_db)

    # 3、对数据进行初步处理，主要是删除重复项
    fd_df = fd_df.drop_duplicates()
    fk_df = fk_df.drop_duplicates()
    # 全部的字段关系
    all_relation_df = fd_df.append(fk_df)
    logging.info('数据读取完成')

    # 4、去重之后的FD关系和FK关系进行开始进行维度划分
    # 4-1、处理FK关系，对FK关系进行扩展，可以进行FK关系互推的将其合并为一个节点
    logging.info('外键关系互推节点合并开始')
    unexpand_fk_dict = {}
    # 遍历外键，得到fk_dict，key为外键关系左部，value为列表，列表中是外键关系右部
    for index, row in fk_df.iterrows():
        if row['LEFT'] in unexpand_fk_dict.keys():
            unexpand_fk_dict[row['LEFT']].append(row['RIGHT'])
        else:
            unexpand_fk_dict[row['LEFT']] = [row['RIGHT']]

    # 对fk_dict进行处理，找出每个左部节点的全部的外键关系，即每个节点通过外键关系可以推出的全部节点，这样如果有互推关系，A字段FK关系推B，B字段FK关系推A，则能扩展出A能推出A
    expand_fk_dict = expand_fk_relation(unexpand_fk_dict)

    # 基于扩展后的FK关系，得到FK互推的字段
    dfeo_fd_dict = get_derive_from_each_other_fk(expand_fk_dict)

    # 修改关系，合并可互推节点，可以互推的节点视为一个节点
    if os.path.exists('../tmp/after_merge_fk_df.csv') and os.path.exists('../tmp/after_merge_all_rela_df.csv'):
        logging.info('已存在修改完成的关系，直接读取')
        fk_df = pd.read_csv('../tmp/after_merge_fk_df.csv')
        all_relation_df = pd.read_csv('../tmp/after_merge_all_rela_df.csv')
    else:
        for key in dfeo_fd_dict.keys():
            fk_df.loc[fk_df.LEFT == key, 'LEFT'] = dfeo_fd_dict[key]
            fk_df.loc[fk_df.RIGHT == key, 'RIGHT'] = dfeo_fd_dict[key]
            all_relation_df.loc[all_relation_df.LEFT == key, 'LEFT'] = dfeo_fd_dict[key]
            all_relation_df.loc[all_relation_df.RIGHT == key, 'RIGHT'] = dfeo_fd_dict[key]
        fk_df.to_csv('../tmp/after_merge_fk_df.csv', index_label='index_label')
        all_relation_df.to_csv('../tmp/after_merge_all_rela_df.csv', index_label='index_label')
    logging.info('外键关系互推节点合并完成')

    # 4-2、删除fk和所有关系中，指向自身的关系
    logging.info('删除fk和所有关系中指向自身的fk关系开始')
    fk_drop_index = []
    for index, row in fk_df.iterrows():
        if row['LEFT'] == row['RIGHT']:
            fk_drop_index.append(index)
    fk_df = fk_df.drop(fk_drop_index, axis=0)
    all_rela_drop_index = []
    for index, row in all_relation_df.iterrows():
        if row['LEFT'] == row['RIGHT']:
            all_rela_drop_index.append(index)
    all_relation_df = all_relation_df.drop(all_rela_drop_index, axis=0)
    logging.info('已删除fk和所有关系中指向自身的fk关系')

    # 5、初步找出维度主节点，原则是：外键关系只出不进的认为是维度主节点
    logging.info('初步找出维度主节点开始')
    if os.path.exists('../tmp/candidate_dim_node_list.pickle'):
        with open('../tmp/candidate_dim_node_list.pickle', 'rb') as p:
            candidate_dim_node_list = pickle.load(p)
    else:
        candidate_dim_node_list = find_candidate_dim_node(fk_df)
        with open('../tmp/candidate_dim_node_list.pickle', 'wb') as p:
            pickle.dump(candidate_dim_node_list, p)
    logging.info('已初步找出维度主节点')

    # 6、整理fd和fk关系,为寻找维度主节点的属性集合和子集集合做准备
    # key为fd关系左部节点，value为fd关系左部节点能够推出的右部节点set集合
    after_arrange_fd_dict = {}
    # key为fk关系左部节点,value为fk关系左部节点能够推出的右部节点set集合
    after_arrange_fk_dict = {}
    for index, row in all_relation_df.iterrows():
        if row['RL'] == 'FD':
            if row['LEFT'] in after_arrange_fd_dict.keys():
                # 求并集，即A字段FD推B字段，A字段FD推C字段...
                after_arrange_fd_dict[row['LEFT']] = after_arrange_fd_dict[row['LEFT']] | {row['RIGHT']}
            else:
                after_arrange_fd_dict[row['LEFT']] = {row['RIGHT']}
        elif row['RL'] == 'FK':
            if row['LEFT'] in after_arrange_fk_dict.keys():
                # 求并集，即A字段FK推B字段，A字段FK推C字段...
                after_arrange_fk_dict[row['LEFT']] = after_arrange_fk_dict[row['LEFT']] | {row['RIGHT']}
            else:
                after_arrange_fk_dict[row['LEFT']] = {row['RIGHT']}
        else:
            logging.error("系统无法识别{}关系，无法进行维度划分".format(row['RL']))
            ibm_db.close(output_conn)
            exit(-1)
    logging.info('函数依赖关系和外键关系整理完毕')

    # 所有候选维度主节点的全部属性关系，[(dim_node, attr1),(dim_node, attr2),...]
    all_attr_rela_list = []
    # 所有候选维度主节点的全部子集关系，[(dim_node, subset1),(dim_node, subset2),...]
    all_subset_rela_list = []
    # 所有候选维度主节点的属性关系和子集关系，{'dim1':[[attr,...],[subset,...]], 'dim2':[[],[]],...}
    all_dim_node_rela = {}

    if os.path.exists('../tmp/all_attr_rela.pickle') and os.path.exists('../tmp/all_subset_rela.pickle') \
            and os.path.exists('../tmp/all_dim_node_rela.pickle'):
        with open('../tmp/all_attr_rela.pickle', 'rb') as p:
            all_attr_rela_list = pickle.load(p)
        with open('../tmp/all_subset_rela.pickle', 'rb') as p:
            all_subset_rela_list = pickle.load(p)
        with open('../tmp/all_dim_node_rela.pickle', 'rb') as p:
            all_dim_node_rela = pickle.load(p)

    # 7、遍历初步判定的维度主节点，找出每个维度节点的属性列表和子集列表，属性列表指的是FD关系，子集列表指的是FK关系
    logging.info('开始寻找每个维度节点的属性列表和子集列表')
    for i in range(len(candidate_dim_node_list)):
        candidate_dim_node = candidate_dim_node_list[i]
        # 如果这个维度主节点分析过了，就跳过不分析
        if candidate_dim_node in all_dim_node_rela.keys():
            continue
        attr_list, subset_list = find_attr_and_subset(candidate_dim_node, after_arrange_fd_dict, after_arrange_fk_dict)
        # 准备把结果写成pickle文件
        all_dim_node_rela[candidate_dim_node] = [attr_list, subset_list]
        for attr in attr_list:
            all_attr_rela_list.append((candidate_dim_node, attr))
        for subset in subset_list:
            all_subset_rela_list.append((candidate_dim_node, subset))
    all_attr_rela_list = list(set(all_attr_rela_list))
    all_subset_rela_list = list(set(all_subset_rela_list))
    with open('../tmp/all_attr_rela.pickle', 'wb') as p:
        pickle.dump(all_attr_rela_list, p)
    with open('../tmp/all_subset_rela.pickle', 'wb') as p:
        pickle.dump(all_subset_rela_list, p)
    with open('../tmp/all_dim_node_rela.pickle', 'wb') as p:
        pickle.dump(all_dim_node_rela, p)
    logging.info('已找出每个维度节点的属性列表和子集列表')

    # 8、检查初步判定的维度节点之间的关系，检查初步判定的维度节点是否可作为维度主节点，原则是如果一个维度节点不做任何节点的属性，或者做两个以上节点的属性，则该节点认定为维度主节点
    dim_main_node_check_res, group_dict, group_num_dict = dim_main_node_check(candidate_dim_node_list,
                                                                              all_attr_rela_list, all_dim_node_rela)

    # 9、对无法作为维度主节点的节点，找到其对应的维度主节点，结果为：key为初步判定的维度节点名，value为True表示该节点为维度主节点，为其他值表示该key不是维度主节点，value是其维度主节点
    candidate_node_find_main_node_res = candidate_node_find_main_node(dim_main_node_check_res)

    # 10、对同表中可以互推的维度主节点进行合并
    logging.info('开始合并相同维度')
    ori_fd_dict = {}
    for index, row in fd_df.iterrows():
        if row['LEFT'] not in ori_fd_dict.keys():
            ori_fd_dict[row['LEFT']] = []
            ori_fd_dict[row['LEFT']].append(row['RIGHT'])
        else:
            ori_fd_dict[row['LEFT']].append(row['RIGHT'])
    # 同表中可以互推的维度节点进行合并
    same_tab_dim_node_merge_res = same_tab_dim_node_merge(candidate_node_find_main_node_res, group_dict, group_num_dict)
    logging.info('已合并相同维度')

    # 11、整理所有节点所属的维度
    logging.info('开始整理所有节点所属的维度')
    res_dict = {'node': [], 'dim': [], 'orig_dim': [], 'type': [], 'del_flag': []}
    # 遍历所有的属性节点，[(dim_node, attr),(dim_node, attr),...]
    for attr in all_attr_rela_list:
        res_dict['node'].append(attr[1])
        # 是维度主节点
        if same_tab_dim_node_merge_res[attr[0]] is True:
            res_dict['dim'].append(attr[0])
            res_dict['orig_dim'].append(attr[0])
        # 不是维度主节点，但是进行了合并，有可能有这种情况，就是同表的维度主节点进行了合并，导致了same_tab_dim_node_merge_res[attr[0]] 不是 True
        else:
            if same_tab_dim_node_merge_res[attr[0]][0] == '#':
                res_dict['dim'].append(same_tab_dim_node_merge_res[attr[0]][1:])
                res_dict['orig_dim'].append(attr[0])
            else:
                res_dict['dim'].append(same_tab_dim_node_merge_res[attr[0]])
                res_dict['orig_dim'].append(same_tab_dim_node_merge_res[attr[0]])
        res_dict['type'].append('attr')
        res_dict['del_flag'].append('1')
    # 遍历所有的子集节点，[(dim_node, subset),(dim_node, subset),...]
    for subset in all_subset_rela_list:
        # 如果子集节点不是某个节点的属性，则只考虑子集关系
        if subset[1] not in res_dict['node']:
            res_dict['node'].append(subset[1])
            if same_tab_dim_node_merge_res[subset[0]] is True:
                res_dict['dim'].append(subset[0])
                res_dict['orig_dim'].append(subset[0])
            else:
                if same_tab_dim_node_merge_res[subset[0]][0] == '#':
                    res_dict['dim'].append(same_tab_dim_node_merge_res[subset[0]][1:])
                    res_dict['orig_dim'].append(subset[0])
                else:
                    res_dict['dim'].append(same_tab_dim_node_merge_res[subset[0]])
                    res_dict['orig_dim'].append(same_tab_dim_node_merge_res[subset[0]])
            res_dict['type'].append('subset')
            res_dict['del_flag'].append('1')
        # 如果子集节点是某个节点的属性，则删除子集关系，保留属性关系
        elif subset[1] in res_dict['node']:
            inds = [ind for ind in range(len(res_dict['node'])) if
                    res_dict['node'][ind] == subset[1] and res_dict['dim'][ind] == subset[0]]
            del_flag = False
            for ind in inds:
                if res_dict['type'][ind] == 'attr':
                    res_dict['del_flag'][ind] = '0'
                    del_flag = True

            if not del_flag:
                res_dict['node'].append(subset[1])
                if same_tab_dim_node_merge_res[subset[0]] is True:
                    res_dict['dim'].append(subset[0])
                    res_dict['orig_dim'].append(subset[0])
                else:
                    if same_tab_dim_node_merge_res[subset[0]][0] == '#':
                        res_dict['dim'].append(same_tab_dim_node_merge_res[subset[0]][1:])
                        res_dict['orig_dim'].append(subset[0])
                    else:
                        res_dict['dim'].append(same_tab_dim_node_merge_res[subset[0]])
                        res_dict['orig_dim'].append(same_tab_dim_node_merge_res[subset[0]])
                res_dict['type'].append('subset')
                res_dict['del_flag'].append('1')

    after_arrange_result_dict = node_arrange(res_dict)
    logging.info('节点归属维度修改完成')

    # 12、保存维度划分结果
    logging.info('保存维度划分结果')
    dim_division_result_df = pd.DataFrame(after_arrange_result_dict)
    dim_division_result_df = dim_division_result_df.drop_duplicates()

    result_code = output_helper.save_dim_division_result(output_conn, conf.output_schema, dim_division_result_df)

    # 如果维度划分结果保存正常，则删除临时文件
    if result_code == 0:
        del_temp_file()
        logging.info('保存维度划分结果完成')
    elif result_code == -1:
        logging.error("维度划分结果保存数据库失败")
    else:
        logging.error("维度划分结果保存数据库返回不支持的状态码")

    # 关闭数据库连接
    ibm_db.close(output_conn)

