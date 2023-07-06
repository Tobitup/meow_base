import os

from setuptools import setup

current_dir = os.path.abspath(os.path.dirname(__file__))


def read_file(path):
    with open(path, "r") as _file:
        return _file.read()


def read_requirements(filename):
    path = os.path.join(current_dir, filename)
    return [req.strip() for req in read_file(path).splitlines() if req.strip()]


with open('README.md', 'r') as readme:
    long_description = readme.read()

module_data = {}
with open(os.path.join(current_dir, "meow_base", "version.py")) as f:
    exec(f.read(), {}, module_data)

setup(
    name=module_data['__name__'],
    version=module_data['__version__'],
    author='David Marchant',
    author_email='d.marchant@ed-alumni.net',
    description='A base framework for MEOW based implementations of workflows',
    long_description=long_description,
    url='https://github.com/PatchOfScotland/meow_base',
    packages=[
        "meow_base",
        "meow_base.conductors",
        "meow_base.core",
        "meow_base.functionality",
        "meow_base.patterns",
        "meow_base.recipes",
    ],
    install_requires=[
        "papermill",
        "nbformat",
        "ipykernel",
        "pyyaml",
        "watchdog"
    ],
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: OS Independent'
    ]
)
