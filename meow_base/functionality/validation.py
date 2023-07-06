
"""
This file contains various validation functions to be used throughout the 
package.

Author(s): David Marchant
"""

from inspect import signature
from os.path import sep, exists, isfile, isdir, dirname
from typing import Any, _SpecialForm, Union, Type, Dict, List, \
    get_origin, get_args

from ..core.vars import VALID_PATH_CHARS, get_not_imp_msg

def check_type(variable:Any, expected_type:Type, alt_types:List[Type]=[], 
        or_none:bool=False, hint:str="")->None:
    """Checks if a given variable is of the expected type. Raises TypeError or
    ValueError as appropriate if any issues are encountered."""

    # Get a list of all allowed types
    type_list = [expected_type]
    if get_origin(expected_type) is Union:
        type_list = list(get_args(expected_type))
    type_list = type_list + alt_types

    # Only accept None if explicitly allowed
    if variable is None:
        if or_none == False:
            if hint:
                msg = f"Not allowed None for {hint}. Expected {expected_type}."
            else:
                msg = f"Not allowed None. Expected {expected_type}."
            raise TypeError(msg)
        else:
            return

    # If any type is allowed, then we can stop checking
    if expected_type == Any:
        return

    # Check that variable type is within the accepted type list
    if not isinstance(variable, tuple(type_list)):
        if hint:
            msg = f"Expected type(s) for {hint} are '{type_list}', " \
                  f"got {type(variable)}"
        else:
            msg = f"Expected type(s) are '{type_list}', got {type(variable)}"
        raise TypeError(msg)

def check_types(variable:Any, expected_types:List[Type], or_none:bool=False, 
        hint:str="")->None:
    """Checks if a given variable is one of the expected types. Raises 
    TypeError or ValueError as appropriate if any issues are encountered."""
    check_type(variable, expected_types[0], alt_types=expected_types[1:],
                or_none=or_none, hint=hint)

def check_callable(call:Any, hint:str="")->None:
    """Checks if a given variable is a callable function. Raises TypeError if 
    not."""
    if not callable(call):
        if hint:
            raise TypeError(
                f"Given object '{call}' by '{hint}' is not a callable function"
            )
        else:
            raise TypeError(
                f"Given object '{call}' is not a callable function"
            )

def check_implementation(child_func, parent_class):
    """Checks if the given function has been overridden from the one inherited
    from the parent class. Raises a NotImplementedError if this is the case."""
    # Check parent first implements func to measure against
    if not hasattr(parent_class, child_func.__name__):
        raise AttributeError(
            f"Parent class {parent_class} does not implement base function "
            f"{child_func.__name__} for children to override.")
    parent_func = getattr(parent_class, child_func.__name__)

    # Check child implements function with correct name
    if (child_func == parent_func):
        msg = get_not_imp_msg(parent_class, parent_func)
        raise NotImplementedError(msg)

    # Check that child implements function with correct signature
    child_sig = signature(child_func).parameters
    parent_sig = signature(parent_func).parameters
    if child_sig.keys() != parent_sig.keys():
        msg = get_not_imp_msg(parent_class, parent_func)
        raise NotImplementedError(msg)

def check_script(script:Any):
    """Checks if a given variable is a valid script. Raises TypeError if 
    not."""
    # TODO investigate more robust check here
    check_type(script, list)
    for line in script:
        check_type(line, str)

def valid_string(variable:str, valid_chars:str, min_length:int=1, hint:str=""
        )->None:
    """Checks that all characters in a given string are present in a provided
    list of characters. Will raise an ValueError if unexpected character is
    encountered."""
    check_type(variable, str, hint=hint)
    check_type(valid_chars, str, hint=hint)

    # Check string is long enough
    if len(variable) < min_length:
        if hint:
            msg = f"String '{variable}' for '{hint}' is too short. Minimum " \
                  f"length is {min_length}"
        else:
            msg = f"String '{variable}' is too short. Minimum length is " \
                  f"{min_length}"
        raise ValueError (msg)

    # Check each char is acceptable
    for char in variable:
        if char not in valid_chars:
            if hint :
                msg = f"Invalid character '{char}' in '{hint}'. Only valid " \
                      f"characters are: {valid_chars}"
            else:
                msg = f"Invalid character '{char}'. Only valid characters " \
                      f"are: {valid_chars}"
            raise ValueError(msg)

def valid_dict(variable:Dict[Any, Any], key_type:Type, value_type:Type, 
        required_keys:List[Any]=[], optional_keys:List[Any]=[], 
        strict:bool=True, min_length:int=1, hint:str="")->None:
    """Checks that a given dictionary is valid. Key and Value types are 
    enforced, as are required and optional keys. Will raise ValueError, 
    TypeError or KeyError depending on the problem encountered."""
    # Validate inputs
    check_type(variable, Dict, hint=hint)
    check_type(key_type, Type, alt_types=[_SpecialForm], hint=hint)
    check_type(value_type, Type, alt_types=[_SpecialForm], hint=hint)
    check_type(required_keys, list, hint=hint)
    check_type(optional_keys, list, hint=hint)
    check_type(strict, bool, hint=hint)

    if hint:
        hint = f"in '{hint}' "

    # Check dict meets minimum length
    if len(variable) < min_length:
        raise ValueError(
            f"Dictionary '{variable}' {hint}is below minimum length of "
            f"{min_length}"
        )

    # Check key and value types
    for k, v in variable.items():
        if key_type != Any and not isinstance(k, key_type):
            raise TypeError(f"Key {k} {hint}had unexpected type '{type(k)}' "
                f"rather than expected '{key_type}' in dict '{variable}'")
        if value_type != Any and not isinstance(v, value_type):
            raise TypeError(f"Value {v} {hint}had unexpected type '{type(v)}' "
                f"rather than expected '{value_type}' in dict '{variable}'")

    # Check all required keys present
    for rk in required_keys:
        if rk not in variable.keys():
            raise KeyError(f"Missing required key '{rk}' from dict "
                f"'{variable}' {hint}.")
    
    # If strict checking, enforce that only required and optional keys are 
    # present
    if strict:
        for k in variable.keys():
            if k not in required_keys and k not in optional_keys:
                raise ValueError(f"Unexpected key '{k}' {hint}should not be "
                                 f"present in dict '{variable}'")

def valid_dict_multiple_types(variable:Dict[Any, Any], key_type:Type, 
        value_types:List[Type], required_keys:List[Any]=[], 
        optional_keys:List[Any]=[], strict:bool=True, min_length:int=1, 
        hint:str="")->None:
    """Checks that a given dictionary is valid. Key and Value types are 
    enforced, as are required and optional keys. Will raise ValueError, 
    TypeError or KeyError depending on the problem encountered."""
    # Validate inputs
    check_type(variable, Dict, hint=hint)
    check_type(key_type, Type, alt_types=[_SpecialForm], hint=hint)
    valid_list(value_types, Type, alt_types=[_SpecialForm], hint=hint)
    check_type(required_keys, list, hint=hint)
    check_type(optional_keys, list, hint=hint)
    check_type(strict, bool, hint=hint)

    if hint:
        hint = f"in '{hint}' "

    # Check dict meets minimum length
    if len(variable) < min_length:
        raise ValueError(
            f"Dictionary '{variable}' {hint}is below minimum length of "
            f"{min_length}"
        )

    # Check key and value types
    for k, v in variable.items():
        if key_type != Any and not isinstance(k, key_type):
            raise TypeError(f"Key {k} {hint}had unexpected type '{type(k)}' "
                f"rather than expected '{key_type}' in dict '{variable}'")
        if Any not in value_types and type(v) not in value_types:
            raise TypeError(f"Value {v} {hint}had unexpected type '{type(v)}' "
                f"rather than expected '{value_types}' in dict '{variable}'")

    # Check all required keys present
    for rk in required_keys:
        if rk not in variable.keys():
            raise KeyError(f"Missing required key '{rk}' from dict "
                f"'{variable}' {hint}.")
    
    # If strict checking, enforce that only required and optional keys are 
    # present
    if strict:
        for k in variable.keys():
            if k not in required_keys and k not in optional_keys:
                raise ValueError(f"Unexpected key '{k}' {hint}should not be "
                                 f"present in dict '{variable}'")

def valid_list(variable:List[Any], entry_type:Type,
        alt_types:List[Type]=[], min_length:int=1, hint:str="")->None:
    """Checks that a given list is valid. Value types are checked and a 
    ValueError or TypeError is raised if a problem is encountered."""
    check_type(variable, List, hint=hint)

    # Check length meets minimum
    if len(variable) < min_length:
        if hint:
            msg = f"List '{variable}' is too short in {hint}. Should be at " \
                  f"least of length {min_length}"
        else:
            msg = f"List '{variable}' is too short. Should be at least " \
                  f"of length {min_length}"
        raise ValueError(msg)
    
    # Check type of each value
    for n, entry in enumerate(variable):
        if hint:
            check_type(entry, entry_type, alt_types=alt_types,
                hint=f"{hint}[{n}]")
        else:
            check_type(entry, entry_type, alt_types=alt_types)

def valid_path(variable:str, allow_base:bool=False, extension:str="", 
        min_length:int=1, hint:str=""):
    """Check that a given string expresses a valid path."""
    valid_string(variable, VALID_PATH_CHARS, min_length=min_length, hint=hint)
    
    # Check we aren't given a root path
    if not allow_base and variable.startswith(sep):
        if hint:
            msg = f"Cannot accept path '{variable}' in '{hint}'. Must be " \
                  "relative."
        else:
            msg = f"Cannot accept path '{variable}'. Must be relative."
        raise ValueError(msg)
    
    # Check path contains a valid extension
    if extension and not variable.endswith(extension):
        if hint:
            msg = f"Path '{variable}' in '{hint}' does not have required " \
                  f"extension '{extension}'."
        else:
            msg = f"Path '{variable}' does not have required extension " \
                  f"'{extension}'."
        raise ValueError(msg)

def valid_existing_file_path(variable:str, allow_base:bool=False, 
        extension:str="", hint:str=""):
    """Check the given string is a path to an existing file."""
    # Check that the string is a path
    valid_path(variable, allow_base=allow_base, extension=extension, hint=hint)
    # Check the path exists
    if not exists(variable):
        if hint:
            msg = f"Requested file path '{variable}' in '{hint}' does not " \
                  "exist."
        else:
            msg = f"Requested file path '{variable}' does not exist."
        raise FileNotFoundError(msg)
    # Check it is a file
    if not isfile(variable):
        if hint: 
            msg = f"Requested file '{variable}' in '{hint}' is not a file."
        else:
            msg = f"Requested file '{variable}' is not a file."
        raise ValueError(msg)

def valid_existing_dir_path(variable:str, allow_base:bool=False, 
        extension:str="", hint:str=""):
    """Check the given string is a path to an existing dir."""
    # Check that the string is a path
    valid_path(variable, allow_base=allow_base, extension=extension, hint=hint)
    # Check the path exists
    if not exists(variable):
        if hint:
            msg = f"Requested dir path '{variable}' in '{hint}' does not " \
                  "exist."
        else:
            msg = f"Requested dir path '{variable}' does not exist."
        raise FileNotFoundError(msg)
    # Check it is a dir
    if not isdir(variable):
        if hint: 
            msg = f"Requested dir '{variable}' in '{hint}' is not a dir."
        else:
            msg = f"Requested dir '{variable}' is not a dir."
        raise ValueError(msg)

def valid_dir_path(variable:str, must_exist:bool=False, allow_base:bool=False,
        hint:str="")->None:
    """Check the given string is a valid directory path, either to an existing 
    one or a location that could contain one."""
    # Check that the string is a path
    valid_path(variable, allow_base=allow_base, extension="", hint=hint)
    # Check the path exists
    does_exist = exists(variable)
    if must_exist and not does_exist:
        if hint:
            msg = f"Requested dir path '{variable}' in '{hint}' does not " \
                  "exist."
        else:
            msg = f"Requested dir path '{variable}' does not exist."
        raise FileNotFoundError(msg)
    # Check it is a directory
    if does_exist and not isdir(variable):
        if hint:
            msg = f"Requested dir '{variable}' in '{hint}' is not a directory."
        else:
            msg = f"Requested dir '{variable}' is not a directory."
        raise ValueError()

def valid_non_existing_path(variable:str, allow_base:bool=False, hint:str=""
        )->None:
    """Check the given string is a path to something that does not exist."""
    # Check that the string is a path
    valid_path(variable, allow_base=allow_base, extension="", hint=hint)
    # Check the path does not exist
    if exists(variable):
        if hint:
            msg = f"Requested path '{variable}' in '{hint}' already exists."
        else:
            msg = f"Requested path '{variable}' already exists."
        raise ValueError(msg)
    # Check that any intermediate directories exist
    if dirname(variable) and not exists(dirname(variable)):
        if hint:
            msg = f"Route to requested path '{variable}' in '{hint}' does " \
                  "not exist."
        else:
            msg = f"Route to requested path '{variable}' does not exist."
        raise ValueError(msg)

def valid_natural(num:int, hint:str="")->None:
    """Check a given value is a natural number. Will raise a ValueError if not."""
    check_type(num, int, hint=hint)

    if num < 0:
        if hint :
            msg = f"Value {num} in {hint} is not a natural number."
        else:
            msg = f"Value {num} is not a natural number."

        raise ValueError(msg)

# TODO add validation for requirement functions