"""
This file contains functions and definitions used in pattern and recipe 
requirements, and assessed by handlers and conductors.

Author(s): David Marchant
"""

from importlib.metadata import version, PackageNotFoundError
from importlib.util import find_spec
from os.path import basename
from sys import version_info, prefix, base_prefix
from typing import Any, Dict, List, Tuple, Union

from .validation import check_type

REQUIREMENT_PYTHON = "python"
REQ_PYTHON_MODULES = "modules"
REQ_PYTHON_VERSION = "version"
REQ_PYTHON_ENVIRONMENT = "environment"

def create_requirement_dict(key:str, entires:Dict[str,Any]
        )->Tuple[str,Dict[str,Any]]:
    return key, entires

def create_python_requirements(modules:Union[str,List[str]]="", 
        version:str="", environment:str="")->Dict[str,Any]:
    check_type(modules, str, alt_types=[List])
    if not isinstance(modules, List):
        modules = [modules]
    for i, module in enumerate(modules):
        check_type(module, str, hint="create_python_requirement.modules[i]")
    check_type(version, str)
    check_type(environment, str)
    
    python_reqs = {}

    if modules != [""]:
        python_reqs[REQ_PYTHON_MODULES] = modules

    if version:
        python_reqs[REQ_PYTHON_VERSION] = version
    
    if environment:
        python_reqs[REQ_PYTHON_ENVIRONMENT] = environment

    return create_requirement_dict(REQUIREMENT_PYTHON, python_reqs)

def check_requirements(requirements:Dict[str,Any])->bool:
    check_type(requirements, dict, hint="check_requirements.requirements")
    result = True
    reason = ""
    for key, value in SUPPORTERD_REQS.items():
        if key in requirements:
            status, msg = value(requirements[key])
            if not status:
                result = False
                reason = msg
    return result, reason

# TODO integrate me into conductors
def check_python_requirements(reqs:Dict[str,Any])->bool:
    check_type(reqs, dict, 
        hint=f"check_requirements.reqs[{REQUIREMENT_PYTHON}]")

    if REQ_PYTHON_ENVIRONMENT in reqs:
        if base_prefix == prefix:
            return False, "" 
        
        if basename(prefix) != reqs[REQ_PYTHON_ENVIRONMENT]:
            return False, ""
        
    if REQ_PYTHON_MODULES in reqs:
        for module in reqs[REQ_PYTHON_MODULES]:
            module_version = None
            relation = None
            for r in ["==", ">=", "<=", ">", "<"]:
                if r in module:
                    module, module_version = module.split(r)
                    relation = r
                    break

            found_spec = find_spec(module)
            if found_spec is None:
                return False, f"Could not find module '{module}'."

            if module_version is None:
                continue

            try:
                installed_version = version(module)
            except PackageNotFoundError:
                return False, f"Could not find module '{module}'."

            if relation == "==" and module_version != installed_version:
                return (
                    False, 
                    f"Installed {module} version '{installed_version}' "
                    f"differs from requested '{module_version}'"
                )
            
            if relation == ">=" and module_version > installed_version:
                return (
                    False,
                    f"Installed {module} version '{installed_version}' "
                    f"is more that requested '{module_version}'"
                )
            
            if relation == "<=" and module_version < installed_version:
                return (
                    False,
                    f"Installed {module} version '{installed_version}' "
                    f"is less that requested '{module_version}'"
                )
            
            if relation == ">" and module_version > installed_version:
                return (
                    False,
                    f"Installed {module} version '{installed_version}' "
                    f"is more that requested '{module_version}'"
                )
            
            if relation == "<" and module_version < installed_version:
                return (
                    False,
                    f"Installed {module} version '{installed_version}' "
                    f"is less that requested '{module_version}'"
                ) 

    if REQ_PYTHON_VERSION in reqs:
        major, minor, micro = parse_versions(reqs[REQ_PYTHON_VERSION]) 

        msg = f"Avaiable Python version number '{version_info[0]}." \
            f"{version_info[1]}.{version_info[2]}' does not meet requested " \
            f"{reqs[REQ_PYTHON_VERSION]}."
        if major and int(version_info[0]) < major:
            return False, msg
        if minor and int(version_info[0]) <= major \
                and int(version_info[1]) < minor:
            return False, msg
        if micro and int(version_info[0]) <= major \
                and int(version_info[1]) <= minor \
                and int(version_info[2]) < micro:
            return False, msg

    return True, ""

def parse_versions(version:str)->None:
    parts = version.split('.')
    if len(parts) == 1:
        return int(parts[0]), None, None
    elif len(parts) == 2:
        return int(parts[0]), int(parts[1]), None
    else:
        return int(parts[0]), int(parts[1]), int(parts[2])

SUPPORTERD_REQS = {
    REQUIREMENT_PYTHON: check_python_requirements,
}
