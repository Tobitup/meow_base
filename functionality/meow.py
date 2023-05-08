"""
This file contains functions for meow specific functionality.

Author(s): David Marchant
"""

from datetime import datetime
from typing import Any, Dict, Union, List
# Necessary for keyword replacement
from os.path import basename, dirname, splitext

from meow_base.core.base_pattern import BasePattern
from meow_base.core.base_recipe import BaseRecipe
from meow_base.core.rule import Rule
from meow_base.functionality.validation import check_type, valid_dict, \
    valid_list
from meow_base.core.vars import EVENT_PATH, EVENT_RULE, EVENT_TIME, \
    EVENT_TYPE, JOB_CREATE_TIME, JOB_EVENT, JOB_ID, JOB_NOTIFICATIONS, \
    JOB_PATTERN, JOB_RECIPE, JOB_REQUIREMENTS, JOB_RULE, JOB_STATUS, \
    JOB_TYPE, STATUS_CREATING, SWEEP_JUMP, SWEEP_START, SWEEP_STOP
from meow_base.functionality.naming import generate_job_id

# core trigger keyword replacements
KEYWORD_PATH = "{PATH}"
KEYWORD_DIR = "{DIR}"
KEYWORD_FILENAME = "{FILENAME}"
KEYWORD_PREFIX = "{PREFIX}"
KEYWORD_EXTENSION = "{EXTENSION}"
KEYWORD_JOB = "{JOB}"

DEFAULT_KEYWORDS = {
    KEYWORD_PATH: "val.replace(KEYWORD_PATH, event[EVENT_PATH])",
    KEYWORD_DIR: "val.replace(KEYWORD_DIR, dirname(event[EVENT_PATH]))",
    KEYWORD_FILENAME: f"val.replace(KEYWORD_FILENAME, basename(event['{EVENT_PATH}']))",
    KEYWORD_PREFIX: f"val.replace(KEYWORD_PREFIX, splitext(basename(event['{EVENT_PATH}']))[0])",
    KEYWORD_EXTENSION: f"val.replace(KEYWORD_EXTENSION, splitext(basename(event['{EVENT_PATH}']))[1])",
    KEYWORD_JOB: "val.replace(KEYWORD_JOB, job_id)",
}


# TODO make this generic for all event types, currently very tied to file 
# events
def replace_keywords(old_dict:Dict[str,str], job_id:str, event:Dict[str,Any],
        keywords:Dict[str,str]=DEFAULT_KEYWORDS)->Dict[str,str]:
    """Function to replace all MEOW magic words in a dictionary with dynamic 
    values."""
    new_dict = {}

    new_keywords, req_imports = event[EVENT_RULE].pattern.get_additional_replacement_keywords()

    for req_import in req_imports:
        exec(req_import)

    keywords = DEFAULT_KEYWORDS | new_keywords

    for var, val in old_dict.items():
        if isinstance(val, str):
            for keyword, substitution in keywords.items():
                if keyword in val:
                    print(substitution)
                    val = eval(substitution)

            new_dict[var] = val
        else:
            new_dict[var] = val

    return new_dict

def create_parameter_sweep(variable_name:str, start:Union[int,float,complex], 
        stop:Union[int,float,complex], jump:Union[int,float,complex]
        )->Dict[str,Dict[str,Union[int,float,complex]]]:
    """Function to create a valid parameter sweep dict for a givan variable, 
    from start to stop, with a regular jump of jump. This function will perform
    some basic checks to ensure this isn't infinitie in nature as that 
    would lead to unlimited jobs scheduled per event."""
    check_type(variable_name, str, hint="create_parameter_sweep.variable_name")
    check_type(start, int, alt_types=[float, complex])
    check_type(stop, int, alt_types=[float, complex])
    check_type(jump, int, alt_types=[float, complex])

    if jump == 0:
        raise ValueError(
            f"Cannot create sweep with a '{SWEEP_JUMP}' value of zero as this "
            "would be infinite in nature."
        )
    elif jump > 0:
        if not stop > start:
            raise ValueError(
                f"Cannot create sweep with a positive '{SWEEP_JUMP}' "
                "value where the end point is smaller than the start as this "
                "would be infinite in nature."
            )
    elif jump < 0:
        if not stop < start:
            raise ValueError(
                f"Cannot create sweep with a negative '{SWEEP_JUMP}' "
                "value where the end point is smaller than the start as this "
                "would be infinite in nature."
            )

    return {
        variable_name: {
            SWEEP_START: start,
            SWEEP_STOP: stop,
            SWEEP_JUMP: jump
        }
    }

def create_event(event_type:str, path:str, rule:Any, time:float,
        extras:Dict[Any,Any]={})->Dict[Any,Any]:
    """Function to create a MEOW dictionary."""
    return {
        **extras, 
        EVENT_PATH: path, 
        EVENT_TYPE: event_type, 
        EVENT_RULE: rule,
        EVENT_TIME: time
    }

def create_job_metadata_dict(job_type:str, event:Dict[str,Any], 
        extras:Dict[Any,Any]={})->Dict[Any,Any]:
    """Function to create a MEOW job dictionary."""
    job_dict = {
        #TODO compress event?
        JOB_ID: generate_job_id(),
        JOB_EVENT: event,
        JOB_TYPE: job_type,
        JOB_PATTERN: event[EVENT_RULE].pattern.name,
        JOB_RECIPE: event[EVENT_RULE].recipe.name,
        JOB_RULE: event[EVENT_RULE].name,
        JOB_STATUS: STATUS_CREATING,
        JOB_CREATE_TIME: datetime.now(),
        JOB_REQUIREMENTS: event[EVENT_RULE].recipe.requirements,
        JOB_NOTIFICATIONS: event[EVENT_RULE].pattern.notifications
    }

    return {**extras, **job_dict}

def create_rules(patterns:Union[Dict[str,BasePattern],List[BasePattern]], 
        recipes:Union[Dict[str,BaseRecipe],List[BaseRecipe]])->Dict[str,Rule]:
    """Function to create any valid rules from a given collection of patterns 
    and recipes. All inbuilt rule types are considered, with additional 
    definitions provided through the 'new_rules' variable. Note that any 
    provided pattern and recipe dictionaries must be keyed with the 
    corresponding pattern and recipe names."""
    # Validation of inputs
    check_type(patterns, Dict, alt_types=[List], hint="create_rules.patterns")
    check_type(recipes, Dict, alt_types=[List], hint="create_rules.recipes")

    # Convert a pattern list to a dictionary
    if isinstance(patterns, list):
        valid_list(patterns, BasePattern, min_length=0)
        patterns = {pattern.name:pattern for pattern in patterns}
    else:
        # Validate the pattern dictionary
        valid_dict(patterns, str, BasePattern, strict=False, min_length=0)
        for k, v in patterns.items():
            if k != v.name:
                raise KeyError(
                    f"Key '{k}' indexes unexpected Pattern '{v.name}' "
                    "Pattern dictionaries must be keyed with the name of the "
                    "Pattern.")

    # Convert a recipe list into a dictionary
    if isinstance(recipes, list):
        valid_list(recipes, BaseRecipe, min_length=0)
        recipes = {recipe.name:recipe for recipe in recipes}
    else:
        # Validate the recipe dictionary
        valid_dict(recipes, str, BaseRecipe, strict=False, min_length=0)
        for k, v in recipes.items():
            if k != v.name:
                raise KeyError(
                    f"Key '{k}' indexes unexpected Recipe '{v.name}' "
                    "Recipe dictionaries must be keyed with the name of the "
                    "Recipe.")

    # Try to create a rule for each rule in turn
    generated_rules = {}
    for pattern in patterns.values():
        if pattern.recipe in recipes:
            try:
                rule = create_rule(pattern, recipes[pattern.recipe])
                generated_rules[rule.name] = rule
            except TypeError:
                pass
    return generated_rules

def create_rule(pattern:BasePattern, recipe:BaseRecipe)->Rule:
    """Function to create a valid rule from a given pattern and recipe. All 
    inbuilt rule types are considered, with additional definitions provided 
    through the 'new_rules' variable."""
    check_type(pattern, BasePattern, hint="create_rule.pattern")
    check_type(recipe, BaseRecipe, hint="create_rule.recipe")
    
    return Rule(
        pattern, 
        recipe
    )
