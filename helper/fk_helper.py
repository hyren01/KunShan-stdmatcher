from pybloom_live import ScalableBloomFilter


def remove_blank(array):
    """
    删除给定array的空字符串
    """
    i = 0
    new_array = []
    for elem in array:
        if not elem:
            i += 1
            continue
        strip = elem.strip()
        if strip:
            new_array.append(strip)
        else:
            i += 1
    return new_array, i


def generate_bloom(conf, capacity, cursor):
    b = ScalableBloomFilter(initial_capacity=capacity, error_rate=conf.bloom_error_rate)
    while True:
        row = cursor.fetchone()
        if not row:
            break
        if row[0]:
            b.add(row[0].rstrip())
    return b


def generate_mul_col_bloom(conf, capacity, cursor):
    b = ScalableBloomFilter(initial_capacity=capacity, error_rate=conf.bloom_error_rate)
    while True:
        row = cursor.fetchone()
        if not row:
            break
        hash_elem = hash(frozenset([str(elem).rstrip() for elem in row]))
        b.add(hash_elem)
    return b


def get_contains_percent_from_cursor(bloom, cursor):
    total = 0
    contains_num = 0
    f = []
    while True:
        row = cursor.fetchone()
        if not row:
            break
        total += 1
        if row[0]:
            elem = row[0].rstrip()
        else:
            continue
        if elem in bloom:
            contains_num += 1
        else:
            f.append(row[0])
    if total == 0:
        return 0, f
    return contains_num / total, f


def get_contains_percent(bloom, array):
    """
    判断array中元素在布隆过滤器中的比例
    f为一定不在bloom中的元素
    """
    t = 0
    f = []
    for elem in array:
        if not elem:
            continue
        elem = elem.rstrip()
        if elem in bloom:
            t += 1
        else:
            f.append(elem)
    if len(array) == 0:
        return 0, f
    return t / len(array), f
