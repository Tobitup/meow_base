
import unittest

from ..meow_base.core.rule import Rule
from ..meow_base.patterns.file_event_pattern import FileEventPattern
from ..meow_base.recipes.jupyter_notebook_recipe import JupyterNotebookRecipe
from .shared import BAREBONES_NOTEBOOK, setup, teardown

class CorrectnessTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()
        
    # Test Rule created from valid pattern and recipe
    def testRuleCreationMinimum(self)->None:
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)

        Rule(fep, jnr)

    # Test Rule not created with empty name
    def testRuleCreationNoName(self)->None:
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)

        r = Rule(fep, jnr)

        self.assertIsInstance(r.name, str)
        self.assertTrue(len(r.name) > 1)

    # Test Rule not created with invalid name
    def testRuleCreationInvalidName(self)->None:
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)

        with self.assertRaises(TypeError):
            Rule(fep, jnr, name=1)

    # Test Rule not created with invalid pattern
    def testRuleCreationInvalidPattern(self)->None:
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)

        with self.assertRaises(TypeError):
            Rule("pattern", jnr)

    # Test Rule not created with invalid recipe
    def testRuleCreationInvalidRecipe(self)->None:
        fep = FileEventPattern("name", "path", "recipe", "file")

        with self.assertRaises(TypeError):
            Rule(fep, "recipe")

    # Test Rule not created with mismatched recipe
    def testRuleCreationMissmatchedRecipe(self)->None:
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("test_recipe", BAREBONES_NOTEBOOK)

        with self.assertRaises(ValueError):
            Rule(fep, jnr)

    # Test Rule created with valid name
    def testRuleSetupName(self)->None:
        name = "name"
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)

        fejnr = Rule(fep, jnr, name=name)

        self.assertEqual(fejnr.name, name)

    # Test Rule not created with valid pattern
    def testRuleSetupPattern(self)->None:
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)

        fejnr = Rule(fep, jnr)

        self.assertEqual(fejnr.pattern, fep)

    # Test Rule not created with valid recipe
    def testRuleSetupRecipe(self)->None:
        fep = FileEventPattern("name", "path", "recipe", "file")
        jnr = JupyterNotebookRecipe("recipe", BAREBONES_NOTEBOOK)

        fejnr = Rule(fep, jnr)

        self.assertEqual(fejnr.recipe, jnr)

