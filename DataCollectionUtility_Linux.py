# coding: shift-jis
import sys

# tuple は値代入ができないため、一旦listに変換する
def setTupleValue(tpl, index, newValue) :
    tmp = list(tpl)         # tuple to list
    tmp[index] = newValue   # update to the new value
    return tuple(tmp)       # tuple繧剃ｽ懈??

# 重複を除き、ユニーク項目を返す
def unify_list(_list):    
   # Not order preserving    
   unique = set(_list)
   return list(unique)

# list要素をつなげて文字列に変換する   
def concat_list_elements(_list, _concat_str):
    # http://stackoverflow.com/questions/12453580/concatenate-item-in-list-to-strings
    new_str = _concat_str.join(_list)
    return new_str

def split_list_in_half(a_list,_is_reversed_seoncd=False):
    # http://stackoverflow.com/questions/752308/split-list-into-smaller-lists
    half = len(a_list)/2 + 0.5
    A = a_list[:int(half)]
    B = a_list[int(half):]
    if _is_reversed_seoncd:
        B.reverse()
    return A, B

def splitList (lst, bin_size):
    # http://stackoverflow.com/questions/21006494/split-a-list-into-n-items-in-a-sublist-with-remainder-being-added
    it = iter(lst)
    new = [[next(it) for _ in range(bin_size)] for _ in range(len(lst) // bin_size)]
    for i, x in enumerate(it):
        new[i].append(x)
    return new



