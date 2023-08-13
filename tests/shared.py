"""
This file contains shared functions and variables used within multiple tests.

Author(s): David Marchant
"""
import errno
import os
import socket

from distutils.dir_util import copy_tree
from time import sleep
from typing import Any, Dict, List, Tuple

from ..meow_base.core.base_conductor import BaseConductor
from ..meow_base.core.base_handler import BaseHandler
from ..meow_base.core.base_monitor import BaseMonitor
from ..meow_base.core.base_pattern import BasePattern
from ..meow_base.core.base_recipe import BaseRecipe
from ..meow_base.core.vars import DEFAULT_JOB_OUTPUT_DIR, \
    DEFAULT_JOB_QUEUE_DIR, LOCK_EXT
from ..meow_base.functionality.file_io import make_dir, rmtree
from ..meow_base.patterns.file_event_pattern import FileEventPattern
from ..meow_base.recipes.jupyter_notebook_recipe import JupyterNotebookRecipe

# testing 
TEST_DIR = "test_files"
TEST_MONITOR_BASE = "test_monitor_base"
TEST_JOB_QUEUE = "test_job_queue_dir"
TEST_JOB_OUTPUT = "test_job_output"
TEST_DATA = "test_data"

def setup():
    make_dir(TEST_DIR, ensure_clean=True)
    make_dir(TEST_MONITOR_BASE, ensure_clean=True)
    make_dir(TEST_JOB_QUEUE, ensure_clean=True)
    make_dir(TEST_JOB_OUTPUT, ensure_clean=True)
    make_dir(DEFAULT_JOB_OUTPUT_DIR, ensure_clean=True)
    make_dir(DEFAULT_JOB_QUEUE_DIR, ensure_clean=True)

def teardown():
    rmtree(TEST_DIR)
    rmtree(TEST_MONITOR_BASE)
    rmtree(TEST_JOB_QUEUE)
    rmtree(TEST_JOB_OUTPUT)
    rmtree(DEFAULT_JOB_OUTPUT_DIR)
    rmtree(DEFAULT_JOB_QUEUE_DIR)
    rmtree("first")
    for f in [
                "temp_phantom_info.h5", 
                "temp_phantom.h5", 
                f"doesNotExist{LOCK_EXT}"
            ]:
        if os.path.exists(f):
            os.remove(f)

def backup_before_teardown(backup_source:str, backup_dest:str):
    make_dir(backup_dest, ensure_clean=True)
    copy_tree(backup_source, backup_dest)

# Necessary, as the creation of locks is not deterministic
def list_non_locks(dir:str)->List[str]:
    return [f for f in os.listdir(dir) if not f.endswith(LOCK_EXT)]

# Necessary, as the creation of locks is not deterministic
def count_non_locks(dir:str)->int:
    return len(list_non_locks(dir))


# Bash scripts
BAREBONES_BASH_SCRIPT = [
    ""
]
COMPLETE_BASH_SCRIPT = [
    '#!/bin/bash',
    '',
    'num=1000',
    'infile="data"',
    'outfile="output"',
    '',
    'echo "starting"',
    '',
    'read var < $infile',
    'for (( i=0; i<$num; i++ ))',
    'do',
    '    var=$((var+i))',
    'done',
    '',
    'div_by=4',
    'echo $var',
    'result=$((var/div_by))',
    '',
    'echo $result',
    '',
    'mkdir -p $(dirname $outfile)',
    '',
    'echo $result > $outfile',
    '',
    'echo "done"'
]

# Jupyter notebooks
BAREBONES_NOTEBOOK = {
    "cells": [],
    "metadata": {},
    "nbformat": 4,
    "nbformat_minor": 4
}
COMPLETE_NOTEBOOK = {
 "cells": [
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": "# The first cell\n\ns = 0\nnum = 1000"
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": "for i in range(num):\n    s += i"
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": "div_by = 4"
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": "result = s / div_by"
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": "print(result)"
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
APPENDING_NOTEBOOK = {
 "cells": [
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Default parameters values\n",
    "# The line to append\n",
    "extra = 'This line comes from a default pattern'\n",
    "# Data input file location\n",
    "infile = 'start"+ os.path.sep +"alpha.txt'\n",
    "# Output file location\n",
    "outfile = 'first"+ os.path.sep +"alpha.txt'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# load in dataset. This should be a text file\n",
    "with open(infile) as input_file:\n",
    "    data = input_file.read()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Append the line\n",
    "appended = data + '\\n' + extra"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "import os\n",
    "\n",
    "# Create output directory if it doesn't exist\n",
    "output_dir_path = os.path.dirname(outfile)\n",
    "\n",
    "if output_dir_path:\n",
    "    os.makedirs(output_dir_path, exist_ok=True)\n",
    "\n",
    "# Save added array as new dataset\n",
    "with open(outfile, 'w') as output_file:\n",
    "   output_file.write(appended)"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.10.6 (main, Nov 14 2022, 16:10:14) [GCC 11.3.0]"
  },
  "vscode": {
   "interpreter": {
    "hash": "916dbcbb3f70747c44a77c7bcd40155683ae19c65e1c03b4aa3499c5328201f1"
   }
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
ADDING_NOTEBOOK = {
 "cells": [
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Default parameters values\n",
    "# Amount to add to data\n",
    "extra = 10\n",
    "# Data input file location\n",
    "infile = 'example_data"+ os.path.sep +"data_0.npy'\n",
    "# Output file location\n",
    "outfile = 'standard_output"+ os.path.sep +"data_0.npy'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "import numpy as np\n",
    "import os"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# load in dataset. Should be numpy array\n",
    "data = np.load(infile)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Add an amount to all the values in the array\n",
    "added = data + int(float(extra))\n",
    "\n",
    "added"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Create output directory if it doesn't exist\n",
    "output_dir_path = os.path.dirname(outfile)\n",
    "\n",
    "if output_dir_path:\n",
    "    os.makedirs(output_dir_path, exist_ok=True)\n",
    "\n",
    "# Save added array as new dataset\n",
    "np.save(outfile, added)"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.6.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
FILTER_RECIPE = {
 "cells": [
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Variables to be overridden\n",
    "input_image = 'Patch.jpg'\n",
    "output_image = 'Blurred_Patch.jpg'\n",
    "args = {}\n",
    "method = 'BLUR'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Import statements\n",
    "from PIL import Image, ImageFilter\n",
    "import yaml\n",
    "import os"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Read in image to apply filter to\n",
    "im = Image.open(input_image)\n",
    "im"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Dynamically construct the filter command as a string from provided arguments\n",
    "exec_str = 'im.filter(ImageFilter.%s' % method\n",
    "args_str = ', '.join(\"{!s}={!r}\".format(key,val) for (key,val) in args.items())\n",
    "exec_str += '(' + args_str + '))'\n",
    "exec_str"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Apply constructed command as python code\n",
    "filtered = eval(exec_str)\n",
    "filtered"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Create output directory if it doesn't exist\n",
    "output_dir_path = os.path.dirname(output_image)\n",
    "\n",
    "if output_dir_path:\n",
    "    os.makedirs(output_dir_path, exist_ok=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Save output image\n",
    "filtered = filtered.save(output_image)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.6.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
MAKER_RECIPE = {
 "cells": [
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Variables to be overridden\n",
    "meow_dir = 'meow_directory'\n",
    "filter_recipe = 'recipe_filter'\n",
    "input_yaml = 'input.yml'\n",
    "workgroup = '{BASE}'\n",
    "workflows_url = 'https://test-sid.idmc.dk/cgi-sid/jsoninterface.py?output_format=json'\n",
    "workflows_session_id = '*redacted*'\n",
    "\n",
    "# Names of the variables in filter_recipe.ipynb\n",
    "recipe_input_image = 'input_image'\n",
    "recipe_output_image = 'output_image'\n",
    "recipe_args = 'args'\n",
    "recipe_method = 'method'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Imports\n",
    "import yaml\n",
    "import mig_meow as meow\n",
    "import os"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Setup environment variables for meow to workgroup communication\n",
    "os.environ['WORKFLOWS_URL'] = workflows_url\n",
    "os.environ['WORKFLOWS_SESSION_ID'] = workflows_session_id"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Read in configuration data\n",
    "with open(input_yaml, 'r') as yaml_file:\n",
    "    y = yaml.full_load(yaml_file)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Assemble a name for the new Pattern\n",
    "name_str = '%s_%s' % (\n",
    "    y['filter'], '_'.join(\"{!s}_{!r}\".format(key,val) for (key,val) in y['args'].items()))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Create the new Pattern\n",
    "new_pattern = meow.Pattern(name_str)\n",
    "new_pattern.add_recipe(filter_recipe)\n",
    "new_pattern.add_single_input(recipe_input_image, y['input_path'])\n",
    "new_pattern.add_output(recipe_output_image, y['output_path'])\n",
    "new_pattern.add_variable(recipe_method, y['filter'])\n",
    "new_pattern.add_variable(recipe_args, y['args'])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Register the new Pattern with the system.\n",
    "meow.export_pattern_to_vgrid(workgroup, new_pattern)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.6.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
POROSITY_CHECK_NOTEBOOK = {
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Variables that will be overwritten accoring to pattern:"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    f"input_filename = 'foam_ct_data{os.path.sep}foam_016_ideal_CT.npy'\n",
    "output_filedir_accepted = 'foam_ct_data_accepted' \n",
    "output_filedir_discarded = 'foam_ct_data_discarded'\n",
    "porosity_lower_threshold = 0.8\n",
    "utils_path = 'idmc_utils_module.py'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "import numpy as np\n",
    "\n",
    "import importlib\n",
    "import matplotlib.pyplot as plt\n",
    "import os\n",
    "\n",
    "import importlib.util\n",
    "spec = importlib.util.spec_from_file_location(\"utils\", utils_path)\n",
    "utils = importlib.util.module_from_spec(spec)\n",
    "spec.loader.exec_module(utils)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Parameters"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "n_samples = 10000"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Load data\n",
    "ct_data = np.load(input_filename)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "utils.plot_center_slices(ct_data)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "sample_inds=np.random.randint(0, len(ct_data.ravel()), n_samples)\n",
    "n_components=2\n",
    "#Perform GMM fitting on samples from dataset\n",
    "means, stds, weights = utils.perform_GMM_np(\n",
    "    ct_data.ravel()[sample_inds], \n",
    "    n_components, \n",
    "    plot=True, \n",
    "    title='GMM fitted to '+str(n_samples)+' of '\n",
    "    +str(len(ct_data.ravel()))+' datapoints')\n",
    "print('weights: ', weights)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Classify data as 'accepted' or 'dircarded' according to porosity level\n",
    "\n",
    "Text file named according to the dataset will be stored in appropriate directories"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    f"filename_withouth_npy=input_filename.split('{os.path.sep}')[-1].split('.')[0]\n",
    "\n",
    "if np.max(weights)>porosity_lower_threshold:\n",
    "    os.makedirs(output_filedir_accepted, exist_ok=True)\n",
    "    acc_path = os.path.join(output_filedir_accepted, \n",
    "                            filename_withouth_npy+'.txt')\n",
    "    with open(acc_path, 'w') as file:\n",
    "        file.write(str(np.max(weights))+' '+str(np.min(weights)))\n",
    "else:\n",
    "    os.makedirs(output_filedir_discarded, exist_ok=True)\n",
    "    dis_path = os.path.join(output_filedir_discarded, \n",
    "                            filename_withouth_npy+'.txt') \n",
    "    with open(dis_path, 'w') as file:\n",
    "        file.write(str(np.max(weights))+' '+str(np.min(weights)))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.6.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
SEGMENT_FOAM_NOTEBOOK = {
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Variables that will be overwritten accoring to pattern:"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    f"input_filename = 'foam_ct_data_accepted{os.path.sep}foam_016_ideal_CT.txt'\n",
    "input_filedir = 'foam_ct_data'\n",
    "output_filedir = 'foam_ct_data_segmented'\n",
    "utils_path = 'idmc_utils_module.py'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "import numpy as np\n",
    "import importlib\n",
    "import matplotlib.pyplot as plt\n",
    "import os\n",
    "import scipy.ndimage as snd\n",
    "import skimage\n",
    "\n",
    "import importlib.util\n",
    "spec = importlib.util.spec_from_file_location(\"utils\", utils_path)\n",
    "utils = importlib.util.module_from_spec(spec)\n",
    "spec.loader.exec_module(utils)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Segmentation\n",
    "\n",
    "Segmentation method used:\n",
    "\n",
    "- Median filter applied to reduce noise\n",
    "- Otsu thresholding applied to get binary data\n",
    "- Morphological closing performed to remove remaining single-voxel noise\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# importlib.reload(utils)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Parameters"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "median_filter_kernel_size = 2"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Load data"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "filename_withouth_txt=input_filename.split(os.path.sep)[-1].split('.')[0]\n",
    "input_data = os.path.join(input_filedir, filename_withouth_txt+'.npy')\n",
    "\n",
    "ct_data = np.load(input_data)\n",
    "utils.plot_center_slices(ct_data, title = filename_withouth_txt)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Median filtering "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "data_filtered = snd.median_filter(ct_data, median_filter_kernel_size)\n",
    "utils.plot_center_slices(data_filtered, title = filename_withouth_txt+' median filtered')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Otsu thresholding"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "threshold = skimage.filters.threshold_otsu(data_filtered)\n",
    "data_thresholded = (data_filtered>threshold)*1\n",
    "utils.plot_center_slices(data_thresholded, title = filename_withouth_txt+' Otsu thresholded')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Morphological closing"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "data_segmented = (skimage.morphology.binary_closing((data_thresholded==0))==0)\n",
    "utils.plot_center_slices(data_segmented, title = filename_withouth_txt+' Otsu thresholded')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Save data"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "filename_save = filename_withouth_txt+'_segmented.npy'\n",
    "os.makedirs(output_filedir, exist_ok=True)\n",
    "np.save(os.path.join(output_filedir, filename_save), data_segmented)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.6.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
FOAM_PORE_ANALYSIS_NOTEBOOK = {
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Variables that will be overwritten accoring to pattern:"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    f"input_filename = 'foam_ct_data_segmented{os.path.sep}foam_016_ideal_CT_segmented.npy'\n",
    "output_filedir = 'foam_ct_data_pore_analysis'\n",
    "utils_path = 'idmc_utils_module.py'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "import numpy as np\n",
    "import importlib\n",
    "import matplotlib.pyplot as plt\n",
    "import os\n",
    "import scipy.ndimage as snd\n",
    "\n",
    "from skimage.segmentation import watershed\n",
    "from skimage.feature import peak_local_max\n",
    "from matplotlib import cm\n",
    "from matplotlib.colors import ListedColormap, LinearSegmentedColormap\n",
    "\n",
    "import importlib.util\n",
    "spec = importlib.util.spec_from_file_location(\"utils\", utils_path)\n",
    "utils = importlib.util.module_from_spec(spec)\n",
    "spec.loader.exec_module(utils)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# importlib.reload(utils)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Foam pore analysis\n",
    "\n",
    "- Use Watershed algorithm to separate pores\n",
    "- Plot statistics\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Load data"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "data = np.load(input_filename)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "utils.plot_center_slices(data, title = input_filename)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Watershed: Identify separate pores "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "#distance map\n",
    "distance = snd.distance_transform_edt((data==0))\n",
    "\n",
    "#get watershed seeds\n",
    "local_maxi = peak_local_max(distance, indices=False, footprint=np.ones((3, 3, 3)), labels=(data==0))\n",
    "markers = snd.label(local_maxi)[0]\n",
    "\n",
    "#perform watershed pore seapration\n",
    "labels = watershed(-distance, markers, mask=(data==0))\n",
    "\n",
    "## Pore color mad\n",
    "somecmap = cm.get_cmap('magma', 256)\n",
    "cvals=np.random.uniform(0, 1, len(np.unique(labels)))\n",
    "newcmp = ListedColormap(somecmap(cvals))\n",
    "\n",
    "\n",
    "utils.plot_center_slices(-distance, cmap=plt.cm.gray, title='Distances')\n",
    "utils.plot_center_slices(labels, cmap=newcmp, title='Separated pores')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Plot statistics: pore radii"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "volumes = np.array([np.sum(labels==label) for label in np.unique(labels)])\n",
    "volumes.sort()\n",
    "#ignore two largest labels (background and matrix)\n",
    "radii = (volumes[:-2]*3/(4*np.pi))**(1/3) #find radii, assuming spherical pores\n",
    "_=plt.hist(radii, bins=200)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Save plot"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "filename_withouth_npy=input_filename.split(os.path.sep)[-1].split('.')[0]\n",
    "filename_save = filename_withouth_npy+'_statistics.png'\n",
    "\n",
    "fig, ax = plt.subplots(1,3, figsize=(15,4))\n",
    "ax[0].imshow(labels[:,:,np.shape(labels)[2]//2], cmap=newcmp)\n",
    "ax[1].imshow(labels[:,np.shape(labels)[2]//2,:], cmap=newcmp)\n",
    "_=ax[2].hist(radii, bins=200)\n",
    "ax[2].set_title('Foam pore radii')\n",
    "\n",
    "os.makedirs(output_filedir, exist_ok=True)\n",
    "plt.savefig(os.path.join(output_filedir, filename_save))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.6.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
GENERATOR_NOTEBOOK = {
 "cells": [
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# importing the necessary modules\n",
    "import numpy as np\n",
    "import random\n",
    "import os\n",
    "import shutil\n",
    "import importlib.util"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Variables to be overridden\n",
    "dest_dir = 'foam_ct_data'\n",
    "discarded = os.path.join('discarded', 'foam_data_0-big-.npy')\n",
    "utils_path = 'idmc_utils_module.py'\n",
    "gen_path = 'shared.py'\n",
    "test_data = 'test_data'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# import loaded modules\n",
    "u_spec = importlib.util.spec_from_file_location(\"utils\", utils_path)\n",
    "utils = importlib.util.module_from_spec(u_spec)\n",
    "u_spec.loader.exec_module(utils)\n",
    "\n",
    "g_spec = importlib.util.spec_from_file_location(\"gen\", gen_path)\n",
    "gen = importlib.util.module_from_spec(g_spec)\n",
    "g_spec.loader.exec_module(gen)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Other variables, will be kept constant\n",
    "_, _, i, val, vx, vy, vz = os.path.basename(discarded).split('_')\n",
    "vz.replace(\".npy\", \"\")\n",
    "i = int(i)\n",
    "val = int(val)\n",
    "vx = int(vx)\n",
    "vy = int(vy)\n",
    "vz = int(vz)\n",
    "res=3/vz\n",
    "\n",
    "chance_good=1\n",
    "chance_small=0\n",
    "chance_big=0\n",
    "\n",
    "nspheres_per_unit_few=100\n",
    "nspheres_per_unit_ideal=1000\n",
    "nspheres_per_unit_many=10000"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "possible_selection = [nspheres_per_unit_ideal] * chance_good \\\n",
    "    + [nspheres_per_unit_few] * chance_big \\\n",
    "    + [nspheres_per_unit_many] * chance_small\n",
    "random.shuffle(possible_selection)\n",
    "selection = possible_selection[0]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "filename = f\"foam_dataset_{i}_{selection}_{vx}_{vy}_{vz}.npy\"\n",
    "backup_file = os.path.join(test_data, filename)\n",
    "if not os.path.exists(backup_file):\n",
    "    gen.create_foam_data_file(backup_file, selection, vx, vy, vz, res)\n",
    "target_file = os.path.join(dest_dir, filename)\n",
    "shutil.copy(backup_file, target_file)"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.6.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}

# Python scripts
BAREBONES_PYTHON_SCRIPT = [
    ""
]
COMPLETE_PYTHON_SCRIPT = [
    "import os",
    "# Setup parameters",
    "num = 1000",
    "infile = 'somehere"+ os.path.sep +"particular'",
    "outfile = 'nowhere"+ os.path.sep +"particular'",
    "",
    "with open(infile, 'r') as file:",
    "    s = float(file.read())",
    ""
    "for i in range(num):",
    "    s += i",
    "",
    "div_by = 4",
    "result = s / div_by",
    "",
    "print(result)",
    "",
    "os.makedirs(os.path.dirname(outfile), exist_ok=True)",
    "",
    "with open(outfile, 'w') as file:",
    "    file.write(str(result))",
    "",
    "print('done')"
]
MULTI_PYTHON_SCRIPT = [
    "import os",
    "# Setup parameters",
    "num = 4",
    "infile = 'somehere"+ os.path.sep +"particular'",
    "outdir = 'nowhere"+ os.path.sep +"particular'",
    "",
    "with open(infile, 'r') as file:",
    "    data = file.read()",
    "",
    "os.makedirs(outdir, exist_ok=True)",
    "",
    "for i in range(num):",
    "    with open(os.path.join(outdir, str(i)), 'w') as file:",
    "        file.write(data)",
    "",
    "print('done')"
]
IDMC_UTILS_PYTHON_SCRIPT = [
    "import matplotlib.pyplot as plt",
    "from sklearn import mixture",
    "import numpy as np",
    "from skimage.morphology import convex_hull_image",
    "",
    "def saveplot(figpath_and_name, dataset):",
    "",
    "    fig, ax=plt.subplots(1, 3, figsize=(10, 4))",
    "    ax[0].imshow(dataset[dataset.shape[0]//2,:,:])",
    "    ax[1].imshow(dataset[:,dataset.shape[1]//2, :])",
    "    ax[2].imshow(dataset[:,:,dataset.shape[2]//2])",
    "    plt.savefig(figpath_and_name)",
    "",
    "",
    "def slice_by_slice_mask_calc(data):",
    "    '''calculate mask from convex hull of data, slice by slice in x-direction'''",
    "",
    "    mask=np.zeros(data.shape)",
    "    no_slices=data.shape[0]",
    "    for i in range(no_slices):",
    "        xslice=data[i,:,:]",
    "        mask[i,:,:]=convex_hull_image(xslice)",
    "    return mask",
    "",
    "",
    "def plot_center_slices(volume, title='', fig_external=[],figsize=(15,5), cmap='viridis', colorbar=False, vmin=None, vmax=None):",
    "        shape=np.shape(volume)",
    "",
    "        if len(fig_external)==0:",
    "            fig,ax = plt.subplots(1,3, figsize=figsize)",
    "        else:",
    "            fig = fig_external[0]",
    "            ax = fig_external[1]",
    "",
    "        fig.suptitle(title)",
    "        im=ax[0].imshow(volume[:,:, int(shape[2]/2)], cmap=cmap, vmin=vmin, vmax=vmax)",
    "        ax[0].set_title('Center z slice')",
    "        ax[1].imshow(volume[:,int(shape[1]/2),:], cmap=cmap, vmin=vmin, vmax=vmax)",
    "        ax[1].set_title('Center y slice')",
    "        ax[2].imshow(volume[int(shape[0]/2),:,:], cmap=cmap, vmin=vmin, vmax=vmax)",
    "        ax[2].set_title('Center x slice')",
    "",
    "        if colorbar:",
    "            fig.subplots_adjust(right=0.8)",
    "            cbar_ax = fig.add_axes([0.85, 0.15, 0.05, 0.7])",
    "            fig.colorbar(im, cax=cbar_ax)",
    "",
    "",
    "def perform_GMM_np(data_np, n_components, plot=False, n_init=1, nbins=500, title='', fig_external=[], return_labels=False):",
    "",
    "    #reshape data",
    "    n_samples=len(data_np)",
    "    X_train = np.concatenate([data_np.reshape((n_samples, 1)), np.zeros((n_samples, 1))], axis=1)",
    "",
    "    # fit a Gaussian Mixture Model",
    "    clf = mixture.GaussianMixture(n_components=n_components, covariance_type='full', n_init=n_init)",
    "    clf.fit(X_train)",
    "    if clf.converged_!=True:",
    "        print(' !! Did not converge! Converged: ',clf.converged_)",
    "",
    "    labels=clf.predict(X_train)",
    "",
    "    means=[]",
    "    stds=[]",
    "    weights=[]",
    "    for c in range(n_components):",
    "        component=X_train[labels==c][:,0]",
    "        means.append(np.mean(component))",
    "        stds.append(np.std(component))",
    "        weights.append(len(component)/len(data_np))",
    "",
    "    if plot:",
    "        gaussian = lambda x, mu, s, A: A*np.exp(-0.5*(x-mu)**2/s**2)/np.sqrt(2*np.pi*s**2)",
    "",
    "        if len(fig_external)>0:",
    "            fig, ax=fig_external[0], fig_external[1]",
    "        else:",
    "            fig, ax=plt.subplots(1, figsize=(16, 8))",
    "",
    "        hist, bin_edges = np.histogram(data_np, bins=nbins)",
    "        bin_size=np.diff(bin_edges)",
    "        bin_centers = bin_edges[:-1] +  bin_size/ 2",
    "        hist_normed = hist/(n_samples*bin_size) #normalizing to get 1 under graph",
    "        ax.bar(bin_centers,hist_normed, bin_size, alpha=0.5)",
    "        if len(title)>0:",
    "            ax.set_title(title)",
    "        else:",
    "            ax.set_title('Histogram, '+str(n_samples)+' datapoints. ')",
    "",
    "        #COLORMAP WITH EVENLY SPACED COLORS!",
    "        colors=plt.cm.rainbow(np.linspace(0,1,n_components+1))#rainbow, plasma, autumn, viridis...",
    "",
    "        x_vals=np.linspace(np.min(bin_edges), np.max(bin_edges), 500)",
    "",
    "        g_total=np.zeros_like(x_vals)",
    "        for c in range(n_components):",
    "            gc=gaussian(x_vals, means[c], stds[c], weights[c])",
    "            ax.plot(x_vals, gc, color=colors[c], linewidth=2, label='mean=%.2f'%(means[c]))",
    "            ax.arrow(means[c], weights[c], 0, 0.1)",
    "            g_total+=gc",
    "        ax.plot(x_vals, g_total, color=colors[-1], linewidth=2, label='Total Model')",
    "        plt.legend()",
    "",
    "    if return_labels:",
    "        return means, stds, weights, labels",
    "    else:",
    "        return means, stds, weights"
]
GENERATE_PYTHON_SCRIPT = [
    "import numpy as np",
    "import random",
    "import foam_ct_phantom.foam_ct_phantom as foam_ct_phantom",
    "",
    "def generate_foam(nspheres_per_unit, vx, vy, vz, res):",
    "    def maxsize_func(x, y, z):",
    "        return 0.2 - 0.1*np.abs(z)",
    "",
    "    random_seed=random.randint(0,4294967295)",
    "    foam_ct_phantom.FoamPhantom.generate('temp_phantom_info.h5',",
    "                                         random_seed,",
    "                                         nspheres_per_unit=nspheres_per_unit,",
    "                                         maxsize=maxsize_func)",
    "",
    "    geom = foam_ct_phantom.VolumeGeometry(vx, vy, vz, res)",
    "    phantom = foam_ct_phantom.FoamPhantom('temp_phantom_info.h5')",
    "    phantom.generate_volume('temp_phantom.h5', geom)",
    "    dataset = foam_ct_phantom.load_volume('temp_phantom.h5')",
    "",
    "    return dataset",
    "",
    "def create_foam_data_file(filename:str, val:int, vx:int, vy:int, vz:int, res:int):",
    "    dataset = generate_foam(val, vx, vy, vz, res)",
    "    np.save(filename, dataset)",
    "    del dataset"
]
COUNTING_PYTHON_SCRIPT = [
    "import os",
    "",
    "dir_to_count = '.'",
    "",
    "print(f'There are {len(os.listdir(dir_to_count))} files in the directory.')"
]

valid_pattern_one = FileEventPattern(
    "pattern_one", "path_one", "recipe_one", "file_one")
valid_pattern_two = FileEventPattern(
    "pattern_two", "path_two", "recipe_two", "file_two")

valid_recipe_one = JupyterNotebookRecipe(
    "recipe_one", BAREBONES_NOTEBOOK)
valid_recipe_two = JupyterNotebookRecipe(
    "recipe_two", BAREBONES_NOTEBOOK)

def check_port_in_use(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        s.bind(("127.0.0.1", port))
    except socket.error as e:
        if e.errno == errno.EADDRINUSE:
            s.close()
            return True
        else:
            # something else raised the socket.error exception
            print(e)
            s.close()
            return False

    s.close()
    return False

def check_shutdown_port_in_timeout(port, timeout):
    for _ in range(timeout):
        if not check_port_in_use(port):
            return
        sleep(1)
    raise OSError(f"Port {port} not closed")


class SharedTestPattern(BasePattern):
    def _is_valid_recipe(self, recipe: Any) -> None:
        pass

    def _is_valid_parameters(self, parameters: Any) -> None:
        pass

    def _is_valid_output(self, outputs: Any) -> None:
        pass

class SharedTestRecipe(BaseRecipe):
    def _is_valid_recipe(self, recipe: Any) -> None:
        pass

    def _is_valid_parameters(self, parameters: Any) -> None:
        pass

    def _is_valid_requirements(self, requirements: Any) -> None:
        pass

class SharedTestMonitor(BaseMonitor):
    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def _get_valid_pattern_types(self) -> List[type]:
        return [SharedTestPattern]
    
    def _get_valid_recipe_types(self) -> List[type]:
        return [SharedTestRecipe]

class SharedTestHandler(BaseHandler):
    def valid_handle_criteria(self, event: Dict[str, Any]) -> Tuple[bool, str]:
        pass

    def get_created_job_type(self) -> str:
        return "test"

    def create_job_recipe_file(self, job_dir: str, event: Dict[str, Any], params_dict: Dict[str, Any]) -> str:
        return "command"

class SharedTestConductor(BaseConductor):
    def valid_execute_criteria(self, job:Dict[str,Any])->Tuple[bool,str]:
        pass

class EmailHandler:
    def __init__(self) -> None:
        self.messages = []

    async def handle_RCPT(self, server, session, envelope, address, rcpt_options):
        if not address.endswith('localhost'):
            return '550 not relaying to that domain'
        envelope.rcpt_tos.append(address)
        return '250 OK'
    
    async def handle_DATA(self, server, session, envelope):
        self.messages.append(envelope)
        return '250 Message accepted for delivery'
