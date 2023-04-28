
"""
This file contains the base MEOW pattern defintion. This should be inherited 
from for all pattern instances.

Author(s): David Marchant
"""

import itertools

from copy import deepcopy
from typing import Any, Union, Tuple, Dict, List

from meow_base.core.vars import VALID_PATTERN_NAME_CHARS, \
    SWEEP_JUMP, SWEEP_START, SWEEP_STOP, get_drt_imp_msg
from meow_base.functionality.validation import valid_string, check_type, \
    check_implementation, valid_dict


class BasePattern:
    # A unique identifier for the pattern
    name:str
    # An identifier of a recipe
    recipe:str
    # Parameters to be overridden in the recipe
    parameters:Dict[str,Any]
    # Parameters showing the potential outputs of a recipe
    outputs:Dict[str,Any]
    # A collection of variables to be swept over for job scheduling
    sweep:Dict[str,Any]
    # TODO Add requirements to patterns
    def __init__(self, name:str, recipe:str, parameters:Dict[str,Any]={}, 
            outputs:Dict[str,Any]={}, sweep:Dict[str,Any]={}):
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
        self._is_valid_sweep(sweep)
        self.sweep = sweep

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
        valid_string(
            name, 
            VALID_PATTERN_NAME_CHARS, 
            hint="BasePattern.name"
        )

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

    def _is_valid_sweep(self, sweep:Dict[str,Union[int,float,complex]])->None:
        """Validation check for 'sweep' variable from main constructor. This 
        function is implemented to check for the types given in the signature, 
        and must be overridden if these differ."""
        check_type(sweep, Dict, hint="BasePattern.sweep")
        if not sweep:
            return
        for _, v in sweep.items():
            valid_dict(
                v, str, Any, [
                    SWEEP_START, SWEEP_STOP, SWEEP_JUMP
                ], strict=True)

            check_type(
                v[SWEEP_START], 
                expected_type=int, 
                alt_types=[float, complex],
                hint=f"BasePattern.sweep[{SWEEP_START}]"
            )
            check_type(
                v[SWEEP_STOP], 
                expected_type=int, 
                alt_types=[float, complex],
                hint=f"BasePattern.sweep[{SWEEP_STOP}]"
            )
            check_type(
                v[SWEEP_JUMP], 
                expected_type=int, 
                alt_types=[float, complex],
                hint=f"BasePattern.sweep[{SWEEP_JUMP}]"
            )
            # Try to check that this loop is not infinite
            if v[SWEEP_JUMP] == 0:
                raise ValueError(
                    f"Cannot create sweep with a '{SWEEP_JUMP}' value of zero"
                )
            elif v[SWEEP_JUMP] > 0:
                if not v[SWEEP_STOP] > v[SWEEP_START]:
                    raise ValueError(
                        f"Cannot create sweep with a positive '{SWEEP_JUMP}' "
                        "value where the end point is smaller than the start."
                    )
            elif v[SWEEP_JUMP] < 0:
                if not v[SWEEP_STOP] < v[SWEEP_START]:
                    raise ValueError(
                        f"Cannot create sweep with a negative '{SWEEP_JUMP}' "
                        "value where the end point is smaller than the start."
                    )

    def assemble_params_dict(self, event:Dict[str,Any]
            )->Union[Dict[str,Any],List[Dict[str,Any]]]:
        """Function to assemble a dictionary of job parameters from this 
        pattern. If no parameter sweeps, then this will be a single dictionary 
        of parameters, otherwise it will be a list of unique dictionaries for 
        every combination of sweeps. This method should be extended by any 
        inheriting pattern classes."""
        yaml_dict = {}

        for var, val in self.parameters.items():
            yaml_dict[var] = val
        for var, val in self.outputs.items():
            yaml_dict[var] = val

        if not self.sweep:
            return yaml_dict

        yaml_dict_list = []
        values_list = self.expand_sweeps()
        for values in values_list:
            for value in values:
                yaml_dict[value[0]] = value[1]
            yaml_dict_list.append(deepcopy(yaml_dict))

        return yaml_dict_list

    def expand_sweeps(self)->List[Tuple[str,Any]]:
        """Function to get all combinations of sweep parameters"""
        values_dict = {}
        # get a collection of a individual sweep values
        for var, val in self.sweep.items():
            values_dict[var] = []
            par_val = val[SWEEP_START]
            while par_val <= val[SWEEP_STOP]:
                values_dict[var].append((var, par_val))
                par_val += val[SWEEP_JUMP]

        # combine all combinations of sweep values
        return list(itertools.product(
            *[v for v in values_dict.values()]))

    def get_additional_replacement_keywords(self):
        return {}
