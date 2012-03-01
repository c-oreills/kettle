from pprint import pformat

def print_indented(format_object, indent=4):
    indent_str = ' ' * indent
    for line in (pformat(format_object, width=79-indent)).split('\n'):
        print indent_str + line
