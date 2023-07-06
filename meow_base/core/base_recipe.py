
"""
This file contains the base MEOW recipe defintion. This should be inherited 
from for all recipe instances.

Author(s): David Marchant
"""

from typing import Any, Dict

from .vars import VALID_RECIPE_NAME_CHARS, \
    get_drt_imp_msg
from ..functionality.validation import check_implementation, \
    valid_string


class BaseRecipe:
    # A unique identifier for the recipe
    name:str
    # Actual code to run
    recipe:Any
    # Possible parameters that could be overridden by a Pattern 
    parameters:Dict[str, Any]
    # Additional configuration options
    requirements:Dict[str, Any]
    def __init__(self, name:str, recipe:Any, parameters:Dict[str,Any]={}, 
            requirements:Dict[str,Any]={}):
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
