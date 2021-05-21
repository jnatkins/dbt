import jinja2
from dbt.clients.jinja import get_environment
from dbt.exceptions import raise_compiler_error


def statically_extract_macro_calls(string, ctx, db_wrapper=None):
    # set 'capture_macros' to capture undefined
    env = get_environment(None, capture_macros=True)
    parsed = env.parse(string)

    standard_calls = ['source', 'ref', 'config']
    possible_macro_calls = []
    for func_call in parsed.find_all(jinja2.nodes.Call):
        func_name = None
        if hasattr(func_call, 'node') and hasattr(func_call.node, 'name'):
            func_name = func_call.node.name
        else:
            # func_call for dbt_utils.current_timestamp macro
            # Call(
            #   node=Getattr(
            #     node=Name(
            #       name='dbt_utils',
            #       ctx='load'
            #     ),
            #     attr='current_timestamp',
            #     ctx='load
            #   ),
            #   args=[],
            #   kwargs=[],
            #   dyn_args=None,
            #   dyn_kwargs=None
            # )
            if (hasattr(func_call, 'node') and
                    hasattr(func_call.node, 'node') and
                    type(func_call.node.node).__name__ == 'Name' and
                    hasattr(func_call.node, 'attr')):
                package_name = func_call.node.node.name
                macro_name = func_call.node.attr
                if package_name == 'adapter':
                    if macro_name == 'dispatch':
                        ad_macro_calls = statically_parse_adapter_dispatch(
                            func_call, ctx, db_wrapper)
                        possible_macro_calls.extend(ad_macro_calls)
                    else:
                        # This skips calls such as adapter.parse_index
                        continue
                else:
                    func_name = f'{package_name}.{macro_name}'
            else:
                continue
        if not func_name:
            continue
        if func_name in standard_calls:
            continue
        elif ctx.get(func_name):
            continue
        else:
            if func_name not in possible_macro_calls:
                possible_macro_calls.append(func_name)

    return possible_macro_calls


# Call(
#   node=Getattr(
#     node=Name(
#       name='adapter',
#       ctx='load'
#     ),
#     attr='dispatch',
#     ctx='load'
#   ),
#   args=[
#     Const(value='test_pkg_and_dispatch')
#   ],
#   kwargs=[
#     Keyword(
#       key='packages',
#       value=Call(node=Getattr(node=Name(name='local_utils', ctx='load'),
#          attr='_get_utils_namespaces', ctx='load'), args=[], kwargs=[],
#          dyn_args=None, dyn_kwargs=None)
#     )
#   ],
#   dyn_args=None,
#   dyn_kwargs=None
# )
def statically_parse_adapter_dispatch(func_call, ctx, db_wrapper):
    possible_macro_calls = []
    # This captures an adapter.dispatch('<macro_name>') call.

    func_name = None
    # macro_name positional argument
    if len(func_call.args) > 0:
        func_name = func_call.args[0].value
    if func_name:
        possible_macro_calls.append(func_name)

    # packages positional argument
    packages = []
    packages_arg = None
    packages_arg_type = None
    
    if len(func_call.args) > 1:
        packages_arg = func_call.args[1]
        # This can be a List or a Call
        packages_arg_type = type(func_call.args[1]).__name__
    
    # keyword arguments
    if func_call.kwargs:
        for kwarg in func_call.kwargs:
            if kwarg.key == 'packages':
                packages_arg = kwarg.value
                # This can be a List or a Call
                packages_arg_type = type(kwarg.value).__name__
            elif kwarg.key == 'macro_name':
                if type(kwarg.value).__name__ == 'Const':
                    func_name = kwarg.value.value
                    possible_macro_calls.append(func_name)
                else:
                    raise_compiler_error(f"The macro_name parameter ({kwarg.value.value}) "
                                         "to adapter.dispatch was not a string")
        
    if packages_arg:
        # Now we might have a 'List', a 'Call', or a 'Const' for a non-call or a var call
        if packages_arg_type == 'List':
            packages = []
            for item in packages_arg.items:
                packages.append(item.value)
        elif packages_arg_type == 'Call':
            if (hasattr(packages_arg, 'node') and
                    hasattr(packages_arg.node, 'node') and
                    hasattr(packages_arg.node.node, 'name') and
                    hasattr(packages_arg.node, 'attr')):
                package_name = packages_arg.node.node.name
                macro_name = packages_arg.node.attr
                if (macro_name.startswith('_get') and 'namespaces' in macro_name):
                    # do the thing
                    var_name = f'{package_name}_dispatch_list'
                    namespace_names = get_dispatch_list(ctx, var_name, [package_name])
                    if namespace_names:
                        packages.extend(namespace_names)
                else:
                    msg = (
                        f"As of v0.19.2, custom macros, such as '{macro_name}', are no longer supported in the 'packages' argument of 'adapter.dispatch()'.\n" \
                        f"See https://docs.getdbt.com/reference/dbt-jinja-functions/dispatch for details."
                    ).strip()
                    raise_compiler_error(msg)
        # I'm not sure if the following logic works as intended, or if we need it for backwards compatibility
        # We should propose a new way of doing this going forward
        elif packages_arg_type == 'Add':
            namespace_var = None
            default_namespaces = []
            # This might be a single call or it might be the 'left' piece in an addition
            for var_call in packages_arg.find_all(jinja2.nodes.Call):
                if (hasattr(var_call, 'node') and
                        var_call.node.name == 'var' and
                        hasattr(var_call, 'args')):
                    namespace_var = var_call.args[0].value
            if hasattr(packages_arg, 'right'):  # we have a default list of namespaces
                for item in packages_arg.right.items:
                    default_namespaces.append(item.value)
            if namespace_var:
                namespace_names = get_dispatch_list(ctx, namespace_var, default_namespaces)
                if namespace_names:
                    packages.extend(namespace_names)
            

    if db_wrapper:
        if not packages:
            packages = None  # empty list behaves differently than None...
        macro = db_wrapper.dispatch(func_name, packages).macro
        func_name = f'{macro.package_name}.{macro.name}'
        possible_macro_calls.append(func_name)
    else:  # this should only be tests
        for package_name in packages:
            possible_macro_calls.append(f'{package_name}.{func_name}')

    return possible_macro_calls


def get_dispatch_list(ctx, var_name, default_packages):
    namespace_list = None
    try:
        # match the logic currently used in package _get_namespaces() macro
        namespace_list = ctx['var'](var_name) + default_packages
    except Exception:
        pass
    namespace_list = namespace_list if namespace_list else default_packages
    return namespace_list
