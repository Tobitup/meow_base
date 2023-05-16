# meow_base
Repository for base MEOW definitions, as well as a some example implementations. 

## Core definitons

The core MEOW defintions are found in **core/meow.py** These are parents classes for implementations to use, with certain attributes and functions enabling a certain amount of interchangeability within those classes.

The most import definitions are the BasePattern, and BaseRecipe, which define the conditions under which processing will take place, and the actuall processing to take place.

## MEOW Runner

The way to run a MEOW system is to create and MeowRunner instance, found in **core/runner.py**. This will take 1 or more Monitors, 1 or more Handlers, and 1 or more Conductors. In turn, these will listen for events, respond to events, and execute any analysis identified. Examples of how this can be run can be found in **tests/testRunner.py**

## Testing
Pytest unittests are provided within the 'tests directory, as well as a script **test_all.sh** for calling all test scripts from a single command. This can be run as:

    test_all.sh -s

or:

    SKIP_LONG=1 pytest . -W ignore::DeprecationWarning

This will skip the more time consuming tests if you just need quick feedback. Without the -s argument then all tests will be run. Individual test scripts can be started using:

    pytest test_runner.py::MeowTests -W ignore::DeprecationWarning

with individual tests runnable with:

    pytest test_runner.py::MeowTests::testMeowRunnerLinkedPythonExecution -W ignore::DeprecationWarning

to run locally, update your '~/.bashrc' file to inclue:

    # Manually added to get local testing of Python files working easier
    export PYTHONPATH=/home/patch/Documents/Research/Python

to generate a coverage report run:

    SKIP_LONG=1 pytest --cov --cov-report=html:../coverage . -W ignore::DeprecationWarning

## Benchmarking 
Benchmarks have been written for this library, but are included in the seperate https://github.com/PatchOfScotland/meow_benchmarks package
