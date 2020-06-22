import re
import logging
import time
import pandas as pd
from configuration import Config
from utils.common_util import date_trans, dynamic_import, comb_lists
from dao.input import tmp_helper


class DimNode:
    """
    用于存储表信息的节点类
    """

    def __init__(self, tab_tup, dim_cols):
        self.tab_tup = tab_tup
        self.dim_cols = dim_cols


class AliasNameGen:
    """
    用于管理和生成表和字段别名的类
    """

    def __init__(self, conn, config, init_tab_num=0, prefix_len=4):
        """
        类构造函数
        :param conn: 输出数据库连接对象
        :param config: 配置对象
        :param init_tab_num: 初始表名生成编号
        """
        self.tab_num = init_tab_num
        self.schema = config.tmp_db
        self.regex = re.compile(r"^t\d{%s}_" % prefix_len)
        self.prefix_len = prefix_len
        self._init_mapping(conn, config)

    def _init_mapping(self, conn, config):
        input_helper, output_helper = dynamic_import(config)
        stat_dict = output_helper.get_config_info(conn, config.output_schema)
        all_table = list(stat_dict.keys())
        mapping_tup, max_id = output_helper.get_alias_mapping(conn, config.output_schema)
        self.origin_to_alias = {origin: alias for alias, sys_code, origin in mapping_tup}
        self.alias_to_origin = {alias: (sys_code, origin) for alias, sys_code, origin in mapping_tup}
        not_in = set(all_table) - set([(sys_code, origin) for alias, sys_code, origin in mapping_tup])
        inserts = []
        for sys_code, origin_name in not_in:
            max_id += 1
            alias = 't' + str(max_id).rjust(self.prefix_len, '0')
            inserts.append((int(max_id), str(sys_code), str(origin_name), str(alias)))
            self.origin_to_alias[origin_name] = alias
            self.alias_to_origin[alias] = (sys_code, origin_name)
        output_helper.save_alias_mapping(conn, inserts, config.output_schema)

    def get_origin_tuple(self, alias_column_name):
        if len(alias_column_name) <= (self.prefix_len + 2):
            return None
        if not self.is_alias_column(alias_column_name):
            return None
        alias = alias_column_name[:self.prefix_len + 1].upper()
        if alias not in self.alias_to_origin:
            return None
        sys_code, origin = self.alias_to_origin[alias]
        col = alias_column_name[self.prefix_len + 2:]
        return sys_code, origin, col

    def generate_tmp_table_name(self):
        """
        生成临时表名称
        :return: 临时表名称
        """
        self.tab_num += 1
        return "%s.calculate_tmp_table_%s" % (self.schema, self.tab_num)

    def generate_alias_column_name(self, origin_table_name, origin_column_name):
        """
        生成字段别名
        :return: 字段别名
        """
        return self.origin_to_alias[origin_table_name] + '_' + origin_column_name

    def is_tmp_table(self, name: str):
        if name.startswith("%s.calculate_tmp_table_" % self.schema):
            return True
        return False

    def is_alias_column(self, name: str):
        if self.regex.match(name):
            return True
        return False


def closures_cycle(fd_relations_list, left_set):
    """

    :param fd_relations_list:
    :param left_set:
    :return:
    """
    new_fd_relations_list = fd_relations_list.copy()
    res = left_set.copy()
    while True:
        flag = True
        for value in new_fd_relations_list:
            if value[0].issubset(res) or '' in value[0]:
                res = {value[1]} | res
                new_fd_relations_list.remove(value)
                flag = False
                break
        if flag:
            break
    return res


def get_alias_col_name(column_tup, generator):
    """
    生成临时字段名
    :param column_tup:
    :param generator:
    :return:
    """
    if generator.is_alias_column(column_tup[2]):
        return column_tup[2]
    return generator.generate_alias_column_name(column_tup[1], column_tup[2])


def generate_tmp_tables(conf, input_conn, output_conn, tables, alias_gen, filter_fks):
    res_fk = []
    filter_fks_set = set(filter_fks)
    input_helper, output_helper = dynamic_import(conf)
    fks = output_helper.get_all_fks(output_conn, conf.output_schema)
    fds = output_helper.get_all_fds(output_conn, conf.output_schema)
    cols_distinct = output_helper.get_columns_distinct(output_conn, conf.output_schema)
    cols_flg = output_helper.get_all_flag_columns(output_conn, conf.output_schema)
    analysis_conf_dict = output_helper.get_config_info(output_conn, conf.output_schema)
    joint_fks = output_helper.get_all_joint_fks(output_conn, conf.output_schema)

    def rm_default_cols(system_code, table_code, columns):
        new_cols = []
        for c in columns:
            if (system_code, table_code, c) in cols_flg:
                continue
            if (system_code, table_code, c) in cols_distinct:
                if int(cols_distinct[(system_code, table_code, c)]) > 1:
                    new_cols.append(c)
            else:
                new_cols.append(c)
        return new_cols

    def process_tmp_table(one_node):
        selects = set()
        # 生成临时表名
        new_tmp_table_name = alias_gen.generate_tmp_table_name()
        alias_fk_col_name = get_alias_col_name(one_node.tab_tup, alias_gen)
        selects.add((one_node.tab_tup[2], alias_fk_col_name))

        alias_cols = []
        for col in one_node.dim_cols:
            if col == one_node.tab_tup[2]:
                continue
            col_tup = (one_node.tab_tup[0], one_node.tab_tup[1], col)
            alias_col_name = get_alias_col_name(col_tup, alias_gen)
            alias_cols.append(alias_col_name)
            selects.add((col, alias_col_name))

        if (one_node.tab_tup[0], one_node.tab_tup[1]) in analysis_conf_dict:
            table_config = analysis_conf_dict[(one_node.tab_tup[0], one_node.tab_tup[1])]
            alg = table_config['ANA_ALG']
            etl_date = table_config['ETL_DATE']
            date_offset = table_config['DATE_OFFSET']
        else:
            logging.ERROR("{}系统中{}表未在配置表找到！".format(one_node.tab_tup[0], one_node.tab_tup[1]))
            raise OSError("{}系统中{}表未在配置表找到！".format(one_node.tab_tup[0], one_node.tab_tup[1]))

        etl_dates = date_trans(etl_date, date_offset)
        if alg == "F5":
            tmp_helper.create_tmp_table(input_conn, new_tmp_table_name, one_node.tab_tup[1], selects, etl_dates[-1:])
        elif alg == "I":
            tmp_helper.create_tmp_table(input_conn, new_tmp_table_name, one_node.tab_tup[1], selects, etl_dates)
        elif alg == "IU":
            # TODO 增加IU方式创建临时表的函数
            pass
        else:
            logging.ERROR("{}表使用了未知算法{}".format(one_node.tab_tup[1], alg))
            raise OSError("{}表使用了未知算法{}".format(one_node.tab_tup[1], alg))
        one_node.tab_tup = ('tmp', new_tmp_table_name, alias_fk_col_name)
        one_node.dim_cols = alias_cols

    # 注：由于主键一定能推出表内所有阶段，所以主键节点应该复用已有节点
    all_pk_nodes = {}  # 存储已经存在的主键节点
    node_table_relations = []
    # 处理单一主键
    for fk in fks:
        if fk['ID'] in filter_fks_set:
            continue
        if (fk['FK_SYS_CODE'], fk['FK_TABLE_CODE']) not in tables or \
                (fk['SYS_CODE'], fk['TABLE_CODE']) not in tables:
            continue

        rel_fk = [((fk['FK_SYS_CODE'], fk['FK_TABLE_CODE'], fk['FK_COL_CODE']),
                   (fk['SYS_CODE'], fk['TABLE_CODE'], fk['COL_CODE']), ('0', fk['ID']), 'fk')]

        if (fk['FK_SYS_CODE'], fk['FK_TABLE_CODE']) not in fds or \
                (fk['SYS_CODE'], fk['TABLE_CODE']) not in fds:
            res_fk += rel_fk
            continue

        where = [(get_alias_col_name((fk['FK_SYS_CODE'], fk['FK_TABLE_CODE'], fk['FK_COL_CODE']), alias_gen),
                  get_alias_col_name((fk['SYS_CODE'], fk['TABLE_CODE'], fk['COL_CODE']), alias_gen))]
        # 取闭包获取所有可推字段
        fk_link_cols = {fk['COL_CODE']}
        fk_tab_fds = []
        for left_fz_set, right_list in fds[(fk['SYS_CODE'], fk['TABLE_CODE'])].items():
            if '' in left_fz_set:
                continue
            for right in right_list:
                fk_tab_fds.append((left_fz_set, right))
        fk_cols_set = closures_cycle(fk_tab_fds, fk_link_cols)
        fk_not_default_cols = list(fk_link_cols) + rm_default_cols(fk['SYS_CODE'], fk['TABLE_CODE'],
                                                                   list(fk_cols_set - fk_link_cols))
        if len(fk_not_default_cols) <= 1:
            res_fk += rel_fk
            continue
        fk_node = DimNode((fk['SYS_CODE'], fk['TABLE_CODE'], fk['COL_CODE']),
                          fk_not_default_cols)
        process_tmp_table(fk_node)

        if (fk['FK_SYS_CODE'], fk['FK_TABLE_CODE'], fk['FK_COL_CODE']) in all_pk_nodes:
            pk_node = all_pk_nodes[(fk['FK_SYS_CODE'], fk['FK_TABLE_CODE'], fk['FK_COL_CODE'])]
        else:
            pk_cols = [tup[2] for tup in cols_distinct if
                       tup[0] == fk['FK_SYS_CODE'] and tup[1] == fk['FK_TABLE_CODE']]
            pk_node = DimNode((fk['FK_SYS_CODE'], fk['FK_TABLE_CODE'], fk['FK_COL_CODE']),
                              rm_default_cols(fk['FK_SYS_CODE'], fk['FK_TABLE_CODE'], pk_cols))
            process_tmp_table(pk_node)
            all_pk_nodes[(fk['FK_SYS_CODE'], fk['FK_TABLE_CODE'], fk['FK_COL_CODE'])] = pk_node
        node_table_relations.append((pk_node, fk_node, where, ('0', fk['ID']), rel_fk))

    # 处理联合主键
    for _, one_joint_fk in joint_fks.items():
        first = one_joint_fk[0]
        if first['GROUP_CODE'] in filter_fks_set:
            continue
        if (first['FK_SYS_CODE'], first['FK_TABLE_CODE']) not in tables or \
                (first['SYS_CODE'], first['TABLE_CODE']) not in tables:
            continue

        where = []
        rel_fk = []
        fk_link_cols = set()
        for fk in one_joint_fk:
            where.append((get_alias_col_name((fk['FK_SYS_CODE'], fk['FK_TABLE_CODE'], fk['FK_COL_CODE']), alias_gen),
                          get_alias_col_name((fk['SYS_CODE'], fk['TABLE_CODE'], fk['COL_CODE']), alias_gen)))
            fk_link_cols.add(fk['COL_CODE'])
            rel_fk.append(((fk['FK_SYS_CODE'], fk['FK_TABLE_CODE'], fk['FK_COL_CODE']),
                           (fk['SYS_CODE'], fk['TABLE_CODE'], fk['COL_CODE']), ('1', fk['GROUP_CODE']), 'fk'))

        if (first['FK_SYS_CODE'], first['FK_TABLE_CODE']) not in fds or \
                (first['SYS_CODE'], first['TABLE_CODE']) not in fds:
            res_fk += rel_fk
            continue
        # 取闭包获取所有可推字段
        fk_tab_fds = []
        for left_fz_set, right_list in fds[(first['SYS_CODE'], first['TABLE_CODE'])].items():
            if '' in left_fz_set:
                continue
            for right in right_list:
                fk_tab_fds.append((left_fz_set, right))
        fk_cols_set = closures_cycle(fk_tab_fds, fk_link_cols)
        fk_not_default_cols = list(fk_link_cols) + rm_default_cols(first['SYS_CODE'], first['TABLE_CODE'],
                                                                   list(fk_cols_set - fk_link_cols))
        if len(fk_not_default_cols) <= len(fk_link_cols):
            res_fk += rel_fk
            continue
        fk_node = DimNode((first['SYS_CODE'], first['TABLE_CODE'], first['COL_CODE']), fk_not_default_cols)
        process_tmp_table(fk_node)
        pk_cols = [tup[2] for tup in cols_distinct if
                   tup[0] == first['FK_SYS_CODE'] and tup[1] == first['FK_TABLE_CODE']]
        pk_node = DimNode((first['FK_SYS_CODE'], first['FK_TABLE_CODE'], first['FK_COL_CODE']),
                          rm_default_cols(first['FK_SYS_CODE'], first['FK_TABLE_CODE'], pk_cols))
        process_tmp_table(pk_node)
        node_table_relations.append((pk_node, fk_node, where, ('1', first['GROUP_CODE']), rel_fk))

    return node_table_relations, res_fk


def multi_check_fd_by_pandas(df, comb_list):
    assert isinstance(df, pd.DataFrame)
    fds = []
    distinct_count = {}

    # level 0
    for r in df.columns:
        c_len = df[r].drop_duplicates().__len__()
        distinct_count[r] = c_len

    # level 1
    for comb in comb_list:
        comb_lower = (str(comb[0]).lower(), str(comb[1]).lower())
        comb_len = df[list(comb_lower)].drop_duplicates().__len__()
        distinct_count[frozenset(comb_lower)] = comb_len

        if comb_len == 0:
            continue
        if distinct_count[comb_lower[0]] == distinct_count[comb_lower[1]] == comb_len:
            fds.append((comb[0], comb[1], 'bfd'))
        elif distinct_count[comb_lower[0]] == comb_len:
            fds.append((comb[0], comb[1], 'fd'))
        elif distinct_count[comb_lower[1]] == comb_len:
            fds.append((comb[1], comb[0], 'fd'))
    return fds


def do_calc_node_table_relations(conf, input_conn, output_conn, node_table_relations, alias_gen, ana_time):
    all_same_set = set()
    not_same_set = set()
    all_relation = []
    input_helper, output_helper = dynamic_import(conf)
    for node_1, node_2, wheres, id_info, rel_fk in node_table_relations:
        selects = set()
        left_selects = []

        wheres_left = [t[0] for t in wheres]
        wheres_right = [t[1] for t in wheres]

        left_pk_alias_col_name = get_alias_col_name(node_1.tab_tup, alias_gen)
        selects.add((node_1.tab_tup, left_pk_alias_col_name))
        left_selects.append((node_1.tab_tup, left_pk_alias_col_name))

        for col in node_1.dim_cols:
            if col == node_1.tab_tup[2]:
                continue
            col_tup = (node_1.tab_tup[0], node_1.tab_tup[1], col)
            alias_col_name = get_alias_col_name(col_tup, alias_gen)
            selects.add((col_tup, alias_col_name))
            left_selects.append((col_tup, alias_col_name))

        right_selects = []
        alias_col_name = get_alias_col_name(node_2.tab_tup, alias_gen)
        right_selects.append((node_2.tab_tup, alias_col_name))
        # selects.add((leaf.tab_tup, alias_col_name))
        for col in node_2.dim_cols:
            if col == node_2.tab_tup[2]:
                continue
            col_tup = (node_2.tab_tup[0], node_2.tab_tup[1], col)
            if col not in wheres_right:
                selects.add((col_tup, alias_col_name))
            alias_col_name = get_alias_col_name(col_tup, alias_gen)
            right_selects.append((col_tup, alias_col_name))

        # 生成临时表名
        new_tmp_table_name = alias_gen.generate_tmp_table_name()
        # 参数
        left_table = node_1.tab_tup[1]
        right_table = node_2.tab_tup[1]

        # 创建临时表
        tmp_helper.create_table_from_inner_joins(input_conn, new_tmp_table_name, left_table, left_selects, right_table,
                                                 right_selects, wheres)

        # 使用长度进行过滤
        selects_alias = [a for _, a in selects]
        lengs_dict = tmp_helper.union_get_max_min_length(input_conn, new_tmp_table_name, selects_alias)
        check_same_set = set()
        for c, c1 in comb_lists([[a for _, a in left_selects if a not in wheres_left],
                                 [a for _, a in right_selects if a not in wheres_right]]):
            if c not in selects_alias or c1 not in selects_alias:
                continue
            leng = lengs_dict[c]
            leng1 = lengs_dict[c1]
            if leng[0] is None or leng[1] is None or leng1[0] is None or leng1[1] is None:
                continue
            if leng1[0] <= leng[0] and leng1[1] >= leng[1]:
                check_same_set.add((c, c1))

        logging.debug("检测同名关系数量：{}".format(str(len(check_same_set))))
        check_same_set = check_same_set - not_same_set - all_same_set
        logging.debug("过滤后检测同名关系数量：{}".format(str(len(check_same_set))))

        check_same_tuples = list(check_same_set)
        same_res = tmp_helper.union_check_same(input_conn, check_same_tuples, new_tmp_table_name)
        res_dict = {}

        for comb, is_same in zip(check_same_tuples, same_res):
            if is_same:
                res_dict[comb] = 'same'
            else:
                not_same_set.add(comb)

        # 对于去空后相等的组合判断不去空是否仍然相等
        logging.debug("检测相等关系")
        check_equals_tuples = list(res_dict.keys())
        equals_res = tmp_helper.union_check_equals(input_conn, check_equals_tuples, new_tmp_table_name)
        for comb, is_equals in zip(check_equals_tuples, equals_res):
            if not is_equals:
                continue
            res_dict[comb] = 'equals'

        count = tmp_helper.get_count(input_conn, new_tmp_table_name)
        if count > 3:
            check_fd_tuples = list(set(comb_lists([[a for _, a in left_selects if a not in wheres_left],
                                                   [a for _, a in right_selects if a not in wheres_right]])) - set(
                res_dict.keys()))
            df = tmp_helper.get_cols_sample(input_conn, new_tmp_table_name, conf.fd_sample_size)
            fds = multi_check_fd_by_pandas(df, check_fd_tuples)
            for c, c1, typ in fds:
                res_dict[(c, c1)] = typ
        else:
            logging.debug("由于join后数据量小于3条。不分析函数依赖")

        decode_rel = []
        for fk in rel_fk:
            decode_rel.append(fk)
        for comb, rel_type in res_dict.items():
            logging.debug("组合：{}，关系类型：{}".format(comb, rel_type))
            all_same_set.add(comb)
            c1_tup = alias_gen.get_origin_tuple(comb[0])
            c2_tup = alias_gen.get_origin_tuple(comb[1])
            decode_rel.append((c1_tup, c2_tup, id_info, rel_type))
        # 进库
        output_helper.save_same_cluster_detail(output_conn, decode_rel, ana_time, conf.output_schema)
        all_relation += decode_rel
        # 清理
        if id_info[0] == '0':
            tmp_helper.clean_tmp_tables(input_conn, [right_table, new_tmp_table_name])
        elif id_info[0] == '1':
            tmp_helper.clean_tmp_tables(input_conn, [left_table, right_table, new_tmp_table_name])

    return all_relation


def merge_same_equals_and_save(conf, output_conn, res_reciprocal_fd, ana_time):
    input_helper, output_helper = dynamic_import(conf)
    cols_distinct = output_helper.get_columns_distinct(output_conn, conf.output_schema)
    res_cluster = list(res_reciprocal_fd.copy())
    temp_save = []
    while True:
        check_set = res_cluster[0]
        flg = True
        for i in range(1, len(res_cluster)):
            other_set = res_cluster[i]
            if len(check_set.intersection(other_set)) > 0:
                new_set = check_set | other_set
                res_cluster.remove(check_set)
                res_cluster.remove(other_set)
                res_cluster.append(new_set)
                flg = False
                break
        if flg:
            temp_save.append(check_set)
            res_cluster.remove(check_set)
        if len(res_cluster) == 0:
            break
    res_cluster = temp_save

    order_res_cluster = []
    for elem_list in res_cluster:
        order_cluster = []
        for elem in elem_list:
            if elem in cols_distinct:
                order_cluster.append((elem, 1, int(cols_distinct[elem])))
            else:
                order_cluster.append((elem, 1, 0))
        order_res_cluster.append(sorted(order_cluster, key=lambda x: x[2], reverse=True))
    new_res_cluster = []

    for cluster_list in order_res_cluster:
        cluster = []
        for i, elem in enumerate(cluster_list):
            cluster.append((elem[0], elem[1], (i + 1)))
        new_res_cluster.append(cluster)
    logging.debug("分析结果:" + str(new_res_cluster))
    return output_helper.save_same_cluster(output_conn, new_res_cluster, ana_time, conf.output_schema)


def run_analyse(conf, input_conn, output_conn, tables, fk_filter=None):
    assert isinstance(conf, Config)
    assert isinstance(tables, list)
    if len(tables) > 0:
        assert isinstance(tables[0], tuple) and len(tables[0]) == 2
    else:
        return

    if fk_filter:
        assert isinstance(fk_filter, list)
    else:
        fk_filter = []

    input_helper, output_helper = dynamic_import(conf)
    ana_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    # 清除临时计算表
    tmp_helper.clean_all_tmp_tables(input_conn, conf.tmp_db)

    alias_generator = AliasNameGen(output_conn, conf)  # 别名生成器
    # 生成临时表
    node_table_relations, res_fk = generate_tmp_tables(conf, input_conn, output_conn, tables,
                                                                  alias_generator, fk_filter)
    output_helper.save_same_cluster_detail(output_conn, res_fk, ana_time, conf.output_schema)

    # 确认关联关系
    res_relations = do_calc_node_table_relations(conf, input_conn, output_conn, node_table_relations,
                                                     alias_generator, ana_time)


def run_cluster(conf, output_conn):
    assert isinstance(conf, Config)

    input_helper, output_helper = dynamic_import(conf)
    ana_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # 清除聚合结果
    output_helper.clean_same_cluster_result(output_conn, conf.output_schema)

    # 查询
    all_same = output_helper.get_same_pair_in_detail(output_conn, conf.output_schema)
    filtered_relations = {frozenset([t[0], t[1]]) for t in all_same}

    # 进库
    return merge_same_equals_and_save(conf, output_conn, filtered_relations, ana_time)
