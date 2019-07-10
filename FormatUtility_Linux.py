class FormatUtility(object):
    """description of class"""

def format_decimal(_number, _digit):
    return "{0:.{digit}f}".format(_number,digit=_digit)

def format_integer(_number, _digit):
    return "{0:{digit}d}".format(_number,digit=_digit)

# snippet
# """ I'm %s years old """ % (10) is out-dated. can cause run-time eror
# """ I'm {0} years old """.format(10) use this instead