"""
This file contains functions for parameterising code in various formats.

Author(s): David Marchant
"""

from copy import deepcopy
from nbformat import validate
from os import getenv
from papermill.translators import papermill_translators
from typing import Any, Dict, List

from .validation import check_script, check_type

# Adapted from: https://github.com/rasmunk/notebook_parameterizer
def parameterize_jupyter_notebook(jupyter_notebook:Dict[str,Any], 
        parameters:Dict[str,Any], expand_env_values:bool=False)->Dict[str,Any]:
    validate(jupyter_notebook)
    check_type(parameters, Dict, 
        hint="parameterize_jupyter_notebook.parameters")

    if jupyter_notebook["nbformat"] != 4:
        raise Warning(
            "Parameterization designed to work with nbformat version 4. "
            f"Differing version of '{jupyter_notebook['nbformat']}' may "
            "produce unexpeted results.")

    # Load input notebook
    if "kernelspec" in jupyter_notebook["metadata"]:
        kernel_name = jupyter_notebook["metadata"]["kernelspec"]["name"]
        language = jupyter_notebook["metadata"]["kernelspec"]["language"]
    if "language_info" in jupyter_notebook["metadata"]:
        kernel_name = jupyter_notebook["metadata"]["language_info"]["name"]
        language = jupyter_notebook["metadata"]["language_info"]["name"]
    else:
        raise AttributeError(
            f"Notebook lacks key language and/or kernel_name attributes "
            "within metadata")

    translator = papermill_translators.find_translator(kernel_name, language)

    output_notebook = deepcopy(jupyter_notebook)

    # Find each
    cells = output_notebook["cells"]
    code_cells = [
        (idx, cell) for idx, cell in enumerate(cells) \
            if cell["cell_type"] == "code"
    ]
    for idx, cell in code_cells:
        cell_updated = False
        source = cell["source"]
        # Either single string or a list of strings
        if isinstance(source, str):
            lines = source.split("\n")
        else:
            lines = source

        for idy, line in enumerate(lines):
            if "=" in line:
                d_line = list(map(lambda x: x.replace(" ", ""), 
                    line.split("=")))
                # Matching parameter name
                if len(d_line) == 2 and d_line[0] in parameters:
                    value = parameters[d_line[0]]
                    # Whether to expand value from os env
                    if (
                        expand_env_values
                        and isinstance(value, str)
                        and value.startswith("ENV_")
                    ):
                        env_var = value.replace("ENV_", "")
                        value = getenv(
                            env_var, 
                            "MISSING ENVIRONMENT VARIABLE: {}".format(env_var)
                        )
                    lines[idy] = translator.assign(
                        d_line[0], translator.translate(value)
                    )

                    cell_updated = True
        if cell_updated:
            cells[idx]["source"] = "\n".join(lines)

    # Validate that the parameterized notebook is still valid
    validate(output_notebook, version=4)

    return output_notebook

def parameterize_python_script(script:List[str], parameters:Dict[str,Any], 
        expand_env_values:bool=False)->Dict[str,Any]:
    check_script(script)
    check_type(parameters, Dict
        ,hint="parameterize_python_script.parameters")

    output_script = deepcopy(script)

    for i, line in enumerate(output_script):
        if "=" in line:
            d_line = list(map(lambda x: x.replace(" ", ""), 
                line.split("=")))
            # Matching parameter name
            if len(d_line) == 2 and d_line[0] in parameters:
                value = parameters[d_line[0]]
                # Whether to expand value from os env
                if (
                    expand_env_values
                    and isinstance(value, str)
                    and value.startswith("ENV_")
                ):
                    env_var = value.replace("ENV_", "")
                    value = getenv(
                        env_var, 
                        "MISSING ENVIRONMENT VARIABLE: {}".format(env_var)
                    )
                output_script[i] = f"{d_line[0]} = {repr(value)}"
                
    # Validate that the parameterized notebook is still valid
    check_script(output_script)

    return output_script

def parameterize_bash_script(script:List[str], parameters:Dict[str,Any], 
        expand_env_values:bool=False)->Dict[str,Any]:
    check_script(script)
    check_type(parameters, Dict
        ,hint="parameterize_bash_script.parameters")

    output_script = deepcopy(script)

    for i, line in enumerate(output_script):
        if "=" in line:
            d_line = list(map(lambda x: x.replace(" ", ""), 
                line.split("=")))
            # Matching parameter name
            if len(d_line) == 2 and d_line[0] in parameters:
                value = parameters[d_line[0]]
                # Whether to expand value from os env
                if (
                    expand_env_values
                    and isinstance(value, str)
                    and value.startswith("ENV_")
                ):
                    env_var = value.replace("ENV_", "")
                    value = getenv(
                        env_var, 
                        "MISSING ENVIRONMENT VARIABLE: {}".format(env_var)
                    )
                output_script[i] = f"{d_line[0]}={repr(value)}"
                
    # Validate that the parameterized notebook is still valid
    check_script(output_script)

    return output_script
