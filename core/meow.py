
"""
This file contains the core MEOW defintions, used throughout this package. 
It is intended that these base definitions are what should be inherited from in
order to create an extendable framework for event-based scheduling and 
processing.

Author(s): David Marchant
"""
import inspect
import sys

from copy import deepcopy
from typing import Any, Union

from core.correctness.vars import VALID_RECIPE_NAME_CHARS, \
    VALID_PATTERN_NAME_CHARS, VALID_RULE_NAME_CHARS, VALID_CHANNELS, \
    get_drt_imp_msg
from core.correctness.validation import valid_string, check_type, \
    check_implementation, valid_list, valid_dict
from core.functionality import generate_id


class BaseRecipe:
    # A unique identifier for the recipe
    name:str
    # Actual code to run
    recipe:Any
    # Possible parameters that could be overridden by a Pattern 
    parameters:dict[str, Any]
    # Additional configuration options
    requirements:dict[str, Any]
    def __init__(self, name:str, recipe:Any, parameters:dict[str,Any]={}, 
            requirements:dict[str,Any]={}):
        """BaseRecipe Constructor. This will check that any class inheriting 
        from it implements its validation functions. It will then call these on
        the input parameters."""
        check_implementation(type(self)._is_valid_recipe, BaseRecipe)
        check_implementation(type(self)._is_valid_parameters, BaseRecipe)
        check_implementation(type(self)._is_valid_requirements, BaseRecipe)
        self._is_valid_name(name)
        self.name = name
        self._is_valid_recipe(recipe)
        self.recipe = recipe
        self._is_valid_parameters(parameters)
        self.parameters = parameters
        self._is_valid_requirements(requirements)
        self.requirements = requirements

    def __new__(cls, *args, **kwargs):
        """A check that this base class is not instantiated itself, only 
        inherited from"""
        if cls is BaseRecipe:
            msg = get_drt_imp_msg(BaseRecipe)
            raise TypeError(msg)
        return object.__new__(cls)

    def _is_valid_name(self, name:str)->None:
        """Validation check for 'name' variable from main constructor. Is 
        automatically called during initialisation. This does not need to be 
        overridden by child classes."""
        valid_string(name, VALID_RECIPE_NAME_CHARS)

    def _is_valid_recipe(self, recipe:Any)->None:
        """Validation check for 'recipe' variable from main constructor. Must 
        be implemented by any child class."""
        pass

    def _is_valid_parameters(self, parameters:Any)->None:
        """Validation check for 'parameters' variable from main constructor. 
        Must be implemented by any child class."""
        pass

    def _is_valid_requirements(self, requirements:Any)->None:
        """Validation check for 'requirements' variable from main constructor. 
        Must be implemented by any child class."""
        pass


class BasePattern:
    # A unique identifier for the pattern
    name:str
    # An identifier of a recipe
    recipe:str
    # Parameters to be overridden in the recipe
    parameters:dict[str,Any]
    # Parameters showing the potential outputs of a recipe
    outputs:dict[str,Any]
    def __init__(self, name:str, recipe:str, parameters:dict[str,Any]={}, 
            outputs:dict[str,Any]={}):
        """BasePattern Constructor. This will check that any class inheriting 
        from it implements its validation functions. It will then call these on
        the input parameters."""
        check_implementation(type(self)._is_valid_recipe, BasePattern)
        check_implementation(type(self)._is_valid_parameters, BasePattern)
        check_implementation(type(self)._is_valid_output, BasePattern)
        self._is_valid_name(name)
        self.name = name
        self._is_valid_recipe(recipe)
        self.recipe = recipe
        self._is_valid_parameters(parameters)
        self.parameters = parameters
        self._is_valid_output(outputs)
        self.outputs = outputs

    def __new__(cls, *args, **kwargs):
        """A check that this base class is not instantiated itself, only 
        inherited from"""
        if cls is BasePattern:
            msg = get_drt_imp_msg(BasePattern)
            raise TypeError(msg)
        return object.__new__(cls)

    def _is_valid_name(self, name:str)->None:
        """Validation check for 'name' variable from main constructor. Is 
        automatically called during initialisation. This does not need to be 
        overridden by child classes."""
        valid_string(name, VALID_PATTERN_NAME_CHARS)

    def _is_valid_recipe(self, recipe:Any)->None:
        """Validation check for 'recipe' variable from main constructor. Must 
        be implemented by any child class."""
        pass

    def _is_valid_parameters(self, parameters:Any)->None:
        """Validation check for 'parameters' variable from main constructor. 
        Must be implemented by any child class."""
        pass

    def _is_valid_output(self, outputs:Any)->None:
        """Validation check for 'outputs' variable from main constructor. Must 
        be implemented by any child class."""
        pass


class BaseRule:
    # A unique identifier for the rule
    name:str
    # A pattern to be used in rule triggering
    pattern:BasePattern
    # A recipe to be used in rule execution
    recipe:BaseRecipe
    # The string name of the pattern class that can be used to create this rule
    pattern_type:str=""
    # The string name of the recipe class that can be used to create this rule
    recipe_type:str=""
    def __init__(self, name:str, pattern:BasePattern, recipe:BaseRecipe):
        """BaseRule Constructor. This will check that any class inheriting 
        from it implements its validation functions. It will then call these on
        the input parameters."""
        check_implementation(type(self)._is_valid_pattern, BaseRule)
        check_implementation(type(self)._is_valid_recipe, BaseRule)
        self._is_valid_name(name)
        self.name = name
        self._is_valid_pattern(pattern)
        self.pattern = pattern
        self._is_valid_recipe(recipe)
        self.recipe = recipe
        self.__check_types_set()

    def __new__(cls, *args, **kwargs):
        """A check that this base class is not instantiated itself, only 
        inherited from"""
        if cls is BaseRule:
            msg = get_drt_imp_msg(BaseRule)
            raise TypeError(msg)
        return object.__new__(cls)

    def _is_valid_name(self, name:str)->None:
        """Validation check for 'name' variable from main constructor. Is 
        automatically called during initialisation. This does not need to be 
        overridden by child classes."""
        valid_string(name, VALID_RULE_NAME_CHARS)

    def _is_valid_pattern(self, pattern:Any)->None:
        """Validation check for 'pattern' variable from main constructor. Must 
        be implemented by any child class."""
        pass

    def _is_valid_recipe(self, recipe:Any)->None:
        """Validation check for 'recipe' variable from main constructor. Must 
        be implemented by any child class."""
        pass

    def __check_types_set(self)->None:
        """Validation check that the self.pattern_type and self.recipe_type 
        attributes have been set in a child class."""
        if self.pattern_type == "":
            raise AttributeError(f"Rule Class '{self.__class__.__name__}' "
                "does not set a pattern_type.")
        if self.recipe_type == "":
            raise AttributeError(f"Rule Class '{self.__class__.__name__}' "
                "does not set a recipe_type.")


class BaseMonitor:
    # A collection of patterns
    _patterns: dict[str, BasePattern]
    # A collection of recipes
    _recipes: dict[str, BaseRecipe]
    # A collection of rules derived from _patterns and _recipes
    _rules: dict[str, BaseRule]
    # A channel for sending messages to the runner. Note that this is not 
    # initialised within the constructor, but within the runner when passed the
    # monitor is passed to it.
    to_runner: VALID_CHANNELS
    def __init__(self, patterns:dict[str,BasePattern], 
            recipes:dict[str,BaseRecipe])->None:
        """BaseMonitor Constructor. This will check that any class inheriting 
        from it implements its validation functions. It will then call these on
        the input parameters."""
        check_implementation(type(self).start, BaseMonitor)
        check_implementation(type(self).stop, BaseMonitor)
        check_implementation(type(self)._is_valid_patterns, BaseMonitor)
        self._is_valid_patterns(patterns)
        check_implementation(type(self)._is_valid_recipes, BaseMonitor)
        self._is_valid_recipes(recipes)
        check_implementation(type(self).add_pattern, BaseMonitor)
        check_implementation(type(self).update_pattern, BaseMonitor)
        check_implementation(type(self).remove_pattern, BaseMonitor)
        check_implementation(type(self).get_patterns, BaseMonitor)
        check_implementation(type(self).add_recipe, BaseMonitor)
        check_implementation(type(self).update_recipe, BaseMonitor)
        check_implementation(type(self).remove_recipe, BaseMonitor)
        check_implementation(type(self).get_recipes, BaseMonitor)
        check_implementation(type(self).get_rules, BaseMonitor)
        # Ensure that patterns and recipes cannot be trivially modified from 
        # outside the monitor, as this will cause internal consistency issues
        self._patterns = deepcopy(patterns)
        self._recipes = deepcopy(recipes)
        self._rules = create_rules(patterns, recipes)
        
    def __new__(cls, *args, **kwargs):
        """A check that this base class is not instantiated itself, only 
        inherited from"""
        if cls is BaseMonitor:
            msg = get_drt_imp_msg(BaseMonitor)
            raise TypeError(msg)
        return object.__new__(cls)

    def _is_valid_patterns(self, patterns:dict[str,BasePattern])->None:
        """Validation check for 'patterns' variable from main constructor. Must
        be implemented by any child class."""
        pass

    def _is_valid_recipes(self, recipes:dict[str,BaseRecipe])->None:
        """Validation check for 'recipes' variable from main constructor. Must 
        be implemented by any child class."""
        pass

    def start(self)->None:
        """Function to start the monitor as an ongoing process/thread. Must be 
        implemented by any child process"""
        pass

    def stop(self)->None:
        """Function to stop the monitor as an ongoing process/thread. Must be 
        implemented by any child process"""
        pass

    def add_pattern(self, pattern:BasePattern)->None:
        """Function to add a pattern to the current definitions. Must be 
        implemented by any child process."""
        pass

    def update_pattern(self, pattern:BasePattern)->None:
        """Function to update a pattern in the current definitions. Must be 
        implemented by any child process."""
        pass

    def remove_pattern(self, pattern:Union[str,BasePattern])->None:
        """Function to remove a pattern from the current definitions. Must be 
        implemented by any child process."""
        pass

    def get_patterns(self)->dict[str,BasePattern]:
        """Function to get a dictionary of all current pattern definitions. 
        Must be implemented by any child process."""
        pass

    def add_recipe(self, recipe:BaseRecipe)->None:
        """Function to add a recipe to the current definitions. Must be 
        implemented by any child process."""
        pass

    def update_recipe(self, recipe:BaseRecipe)->None:
        """Function to update a recipe in the current definitions. Must be 
        implemented by any child process."""
        pass

    def remove_recipe(self, recipe:Union[str,BaseRecipe])->None:
        """Function to remove a recipe from the current definitions. Must be 
        implemented by any child process."""
        pass

    def get_recipes(self)->dict[str,BaseRecipe]:
        """Function to get a dictionary of all current recipe definitions. 
        Must be implemented by any child process."""
        pass

    def get_rules(self)->dict[str,BaseRule]:
        """Function to get a dictionary of all current rule definitions. 
        Must be implemented by any child process."""
        pass


class BaseHandler:
    # A channel for sending messages to the runner. Note that this is not 
    # initialised within the constructor, but within the runner when passed the
    # handler is passed to it.
    to_runner: VALID_CHANNELS
    def __init__(self)->None:
        """BaseHandler Constructor. This will check that any class inheriting 
        from it implements its validation functions."""
        check_implementation(type(self).handle, BaseHandler)
        check_implementation(type(self).valid_event_types, BaseHandler)

    def __new__(cls, *args, **kwargs):
        """A check that this base class is not instantiated itself, only 
        inherited from"""
        if cls is BaseHandler:
            msg = get_drt_imp_msg(BaseHandler)
            raise TypeError(msg)
        return object.__new__(cls)

    def valid_event_types(self)->list[str]:
        """Function to provide a list of the types of events this handler can 
        process. Must be implemented by any child process."""
        pass

    def handle(self, event:dict[str,Any])->None:
        """Function to handle a given event. Must be implemented by any child 
        process."""
        pass


class BaseConductor:
    def __init__(self)->None:
        """BaseConductor Constructor. This will check that any class inheriting
        from it implements its validation functions."""
        check_implementation(type(self).execute, BaseConductor)
        check_implementation(type(self).valid_job_types, BaseConductor)

    def __new__(cls, *args, **kwargs):
        """A check that this base class is not instantiated itself, only 
        inherited from"""
        if cls is BaseConductor:
            msg = get_drt_imp_msg(BaseConductor)
            raise TypeError(msg)
        return object.__new__(cls)

    def valid_job_types(self)->list[str]:
        """Function to provide a list of the types of jobs this conductor can 
        process. Must be implemented by any child process."""
        pass

    def execute(self, job:dict[str,Any])->None:
        """Function to execute a given job. Must be implemented by any child 
        process."""
        pass


def create_rules(patterns:Union[dict[str,BasePattern],list[BasePattern]], 
        recipes:Union[dict[str,BaseRecipe],list[BaseRecipe]],
        new_rules:list[BaseRule]=[])->dict[str,BaseRule]:
    """Function to create any valid rules from a given collection of patterns 
    and recipes. All inbuilt rule types are considered, with additional 
    definitions provided through the 'new_rules' variable. Note that any 
    provided pattern and recipe dictionaries must be keyed with the 
    corresponding pattern and recipe names."""
    # Validation of inputs
    check_type(patterns, dict, alt_types=[list])
    check_type(recipes, dict, alt_types=[list])
    valid_list(new_rules, BaseRule, min_length=0)

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

def create_rule(pattern:BasePattern, recipe:BaseRecipe, 
        new_rules:list[BaseRule]=[])->BaseRule:
    """Function to create a valid rule from a given pattern and recipe. All 
    inbuilt rule types are considered, with additional definitions provided 
    through the 'new_rules' variable."""
    check_type(pattern, BasePattern)
    check_type(recipe, BaseRecipe)
    valid_list(new_rules, BaseRule, min_length=0)

    # Imported here to avoid circular imports at top of file
    import rules
    # Get a dictionary of all inbuilt rules
    all_rules ={(r.pattern_type, r.recipe_type):r for r in [r[1] \
        for r in inspect.getmembers(sys.modules["rules"], inspect.isclass) \
        if (issubclass(r[1], BaseRule))]}

    # Add in new rules
    for rule in new_rules:
        all_rules[(rule.pattern_type, rule.recipe_type)] = rule

    # Find appropriate rule type from pattern and recipe types
    key = (type(pattern).__name__, type(recipe).__name__)
    if (key) in all_rules:
        return all_rules[key](
            generate_id(prefix="Rule_"), 
            pattern, 
            recipe
        )
    # Raise error if not valid rule type can be found
    raise TypeError(f"No valid rule for Pattern '{pattern}' and Recipe "
        f"'{recipe}' could be found.")
