
import unittest
 
from multiprocessing import Pipe

from meow_base.core.base_conductor import BaseConductor
from meow_base.core.base_handler import BaseHandler
from meow_base.core.base_monitor import BaseMonitor
from meow_base.core.base_pattern import BasePattern
from meow_base.core.base_recipe import BaseRecipe
from meow_base.core.vars import SWEEP_STOP, SWEEP_JUMP, SWEEP_START
from meow_base.patterns.file_event_pattern import FileEventPattern
from shared import SharedTestConductor, SharedTestHandler, SharedTestMonitor, \
    SharedTestPattern, SharedTestRecipe, setup, teardown


class BaseRecipeTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test BaseRecipe instantiation
    def testBaseRecipe(self)->None:
        with self.assertRaises(TypeError):
            BaseRecipe("name", "")
        
        class EmptyRecipe(BaseRecipe):
            pass
        with self.assertRaises(NotImplementedError):
            EmptyRecipe("name", "")

        SharedTestRecipe("name", "")

    # test name validation
    def testValidName(self)->None:
        SharedTestRecipe("name", "")

        with self.assertRaises(ValueError):
            SharedTestRecipe("", "")

        with self.assertRaises(TypeError):
            SharedTestRecipe(123, "")

class BasePatternTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test BasePattern instantiation
    def testBasePattern(self)->None:
        with self.assertRaises(TypeError):
            BasePattern("name", "", "", "")

        class EmptyPattern(BasePattern):
            pass
        with self.assertRaises(NotImplementedError):
            EmptyPattern("name", "", "", "")

        SharedTestPattern("name", "", "", "")

    # Test name validation
    def testValidName(self)->None:
        SharedTestPattern("name", "recipe")

        with self.assertRaises(ValueError):
            SharedTestPattern("", "recipe")

        with self.assertRaises(TypeError):
            SharedTestPattern(123, "recipe")

    # test sweep validation
    def testValidSweep(self)->None:
        SharedTestPattern("name", "recipe", sweep={
            "s1":{
                    SWEEP_START: 10, SWEEP_STOP: 20, SWEEP_JUMP:5
                }
            }
        )

        with self.assertRaises(TypeError):
            SharedTestPattern("name", "recipe", sweep={
                SWEEP_START: 10, SWEEP_STOP: 20, SWEEP_JUMP:5
                }
            )

        with self.assertRaises(ValueError):
            SharedTestPattern("name", "recipe", sweep={
                "s1":{
                        SWEEP_START: 10, SWEEP_STOP: 10, SWEEP_JUMP:5
                    }
                }
            )

        with self.assertRaises(ValueError):
            SharedTestPattern("name", "recipe", sweep={
                "s1":{
                        SWEEP_START: 10, SWEEP_STOP: 0, SWEEP_JUMP:5
                    }
                }
            )

        SharedTestPattern("name", "recipe", sweep={
            "s1":{
                    SWEEP_START: 10, SWEEP_STOP: 0, SWEEP_JUMP:-5
                }
            }
        )

        with self.assertRaises(ValueError):
            SharedTestPattern("name", "recipe", sweep={
                "s1":{
                        SWEEP_START: 0, SWEEP_STOP: 10, SWEEP_JUMP:-5
                    }
                }
            )

        with self.assertRaises(ValueError):
            SharedTestPattern("name", "recipe", sweep={
                "s1":{
                        SWEEP_START: 10, SWEEP_STOP: 10, SWEEP_JUMP:-5
                    }
                }
            )

    # Test expansion of parameter sweeps
    def testBasePatternExpandSweeps(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={
                "s1":{
                    SWEEP_START: 10, SWEEP_STOP: 20, SWEEP_JUMP:5
                }
            })

        es = pattern_one.expand_sweeps()

        self.assertIsInstance(es, list)
        self.assertEqual(len(es), 3)

        values = [
            "s1-10", "s1-15", "s1-20", 
        ]

        for sweep_vals in es:
            self.assertIsInstance(sweep_vals, tuple)
            self.assertEqual(len(sweep_vals), 1)

            val1 = None
            for sweep_val in sweep_vals:
                self.assertIsInstance(sweep_val, tuple)
                self.assertEqual(len(sweep_val), 2)
                if sweep_val[0] == "s1":
                    val1 = f"s1-{sweep_val[1]}"
            if val1:
                values.remove(val1)
        self.assertEqual(len(values), 0)

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={
                "s1":{
                    SWEEP_START: 0, SWEEP_STOP: 2, SWEEP_JUMP:1
                },
                "s2":{
                    SWEEP_START: 20, SWEEP_STOP: 80, SWEEP_JUMP:15
                }
            })

        es = pattern_one.expand_sweeps()

        self.assertIsInstance(es, list)
        self.assertEqual(len(es), 15)

        values = [
            "s1-0/s2-20", "s1-1/s2-20", "s1-2/s2-20", 
            "s1-0/s2-35", "s1-1/s2-35", "s1-2/s2-35", 
            "s1-0/s2-50", "s1-1/s2-50", "s1-2/s2-50", 
            "s1-0/s2-65", "s1-1/s2-65", "s1-2/s2-65", 
            "s1-0/s2-80", "s1-1/s2-80", "s1-2/s2-80", 
        ]

        for sweep_vals in es:
            self.assertIsInstance(sweep_vals, tuple)
            self.assertEqual(len(sweep_vals), 2)

            val1 = None
            val2 = None
            for sweep_val in sweep_vals:
                self.assertIsInstance(sweep_val, tuple)
                self.assertEqual(len(sweep_val), 2)
                if sweep_val[0] == "s1":
                    val1 = f"s1-{sweep_val[1]}"
                if sweep_val[0] == "s2":
                    val2 = f"s2-{sweep_val[1]}"
            if val1 and val2:
                values.remove(f"{val1}/{val2}")
        self.assertEqual(len(values), 0)


class BaseMonitorTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test BaseMonitor instantiation
    def testBaseMonitor(self)->None:
        with self.assertRaises(TypeError):
            BaseMonitor({}, {})

        class EmptyTestMonitor(BaseMonitor):
            pass

        with self.assertRaises(NotImplementedError):
            EmptyTestMonitor({}, {})
           
        SharedTestMonitor({}, {})

    # Test new rules created as meow objects added
    def testBaseMonitorIdentifyNewRules(self)->None:
        p1 = SharedTestPattern("p1", "r1")
        p2 = SharedTestPattern("p2", "r4")
        p3 = SharedTestPattern("p3", "r5")
        r1 = SharedTestRecipe("r1", "")
        r2 = SharedTestRecipe("r2", "")
        r3 = SharedTestRecipe("r3", "")

        monitor = SharedTestMonitor(
            {
                p1.name: p1,
                p2.name: p2,
                p3.name: p3
            }, 
            {
                r1.name: r1,
                r2.name: r2,
                r3.name: r3
            }
        )

        self.assertEqual(len(monitor._rules), 1)
        rule_id = list(monitor._rules.keys())[0]
        print(rule_id)
        self.assertEqual(monitor._rules[rule_id].pattern, p1)
        self.assertEqual(monitor._rules[rule_id].recipe, r1)
        existing_rules = [rule_id]

        r4 = SharedTestRecipe("r4", "")
        monitor._recipes[r4.name] = r4
        monitor._identify_new_rules(new_recipe=r4)

        self.assertEqual(len(monitor._rules), 2)
        rule_id = [i for i in list(monitor._rules.keys()) 
                   if i not in existing_rules][0]
        self.assertEqual(monitor._rules[rule_id].pattern.name, p2.name)
        self.assertEqual(monitor._rules[rule_id].recipe, r4)
        existing_rules.append(rule_id)

        p4 = SharedTestPattern("p4", "r2")
        monitor._patterns[p4.name] = p4
        monitor._identify_new_rules(new_pattern=p4)

        self.assertEqual(len(monitor._rules), 3)
        rule_id = [i for i in list(monitor._rules.keys()) 
                   if i not in existing_rules][0]
        self.assertEqual(monitor._rules[rule_id].pattern, p4)
        self.assertEqual(monitor._rules[rule_id].recipe.name, r2.name)
        existing_rules.append(rule_id)

    # test rules removed as meow objects deleted
    def testBaseMonitorIdentifyLostRules(self)->None:
        p1 = SharedTestPattern("p1", "r1")
        p2 = SharedTestPattern("p2", "r2")
        p3 = SharedTestPattern("p3", "r3")
        r1 = SharedTestRecipe("r1", "")
        r2 = SharedTestRecipe("r2", "")
        r3 = SharedTestRecipe("r3", "")

        monitor = SharedTestMonitor(
            {
                p1.name: p1,
                p2.name: p2,
                p3.name: p3
            }, 
            {
                r1.name: r1,
                r2.name: r2,
                r3.name: r3
            }
        )

        self.assertEqual(len(monitor._rules), 3)

        monitor._identify_lost_rules(lost_pattern=p2.name)

        self.assertEqual(len(monitor._rules), 2)

        monitor._identify_lost_rules(lost_recipe=r3.name)

        self.assertEqual(len(monitor._rules), 1)

    # test specific rule creation
    def testBaseMonitorCreateNewRule(self)->None:
        monitor = SharedTestMonitor(
            {}, 
            {}
        )

        self.assertEqual(len(monitor._rules), 0)

        p1 = SharedTestPattern("p1", "r1")
        r1 = SharedTestRecipe("r1", "")

        monitor._create_new_rule(p1, r1)

        self.assertEqual(len(monitor._rules), 1)

    # test monitor can send an event along a pipe
    def testBaseMonitorSendEventToRunner(self)->None:
        monitor = SharedTestMonitor({}, {})
        from_monitor, to_test = Pipe()
        monitor.to_runner_event = to_test

        monitor.send_event_to_runner("test")

        if from_monitor.poll(3):
            msg = from_monitor.recv()

        self.assertEqual(msg, "test")

    # test we can add patterns
    def testBaseMonitorAddPattern(self)->None:
        monitor = SharedTestMonitor({}, {})

        self.assertEqual(len(monitor._patterns), 0)

        p1 = SharedTestPattern("p1", "r1")

        monitor.add_pattern(p1)

        self.assertEqual(len(monitor._patterns), 1)
        self.assertEqual(monitor._patterns[p1.name].name, p1.name)

    # test we can update patterns
    def testBaseMonitorUpdatePattern(self)->None:
        starting_rec = "r1"
        updated_rec = "r2"

        p1 = SharedTestPattern("p1", starting_rec)

        monitor = SharedTestMonitor(
            {
                p1.name: p1
            }, 
            {}
        )

        self.assertEqual(len(monitor._patterns), 1)
        self.assertEqual(monitor._patterns[p1.name].name, p1.name)
        self.assertEqual(monitor._patterns[p1.name].recipe, p1.recipe)

        p1.recipe = updated_rec

        self.assertEqual(len(monitor._patterns), 1)
        self.assertEqual(monitor._patterns[p1.name].name, p1.name)
        self.assertEqual(monitor._patterns[p1.name].recipe, starting_rec)

        monitor.update_pattern(p1)

        self.assertEqual(len(monitor._patterns), 1)
        self.assertEqual(monitor._patterns[p1.name].name, p1.name)
        self.assertEqual(monitor._patterns[p1.name].recipe, p1.recipe)

    # test we can remove patterns
    def testBaseMonitorRemovePattern(self)->None:
        p1 = SharedTestPattern("p1", "r1")

        monitor = SharedTestMonitor(
            {
                p1.name: p1
            }, 
            {}
        )

        self.assertEqual(len(monitor._patterns), 1)
        self.assertEqual(monitor._patterns[p1.name].name, p1.name)

        monitor.remove_pattern(p1)

        self.assertEqual(len(monitor._patterns), 0)

    # test we can retrieve patterns
    def testBaseMonitorGetPatterns(self)->None:
        p1 = SharedTestPattern("p1", "r1")
        p2 = SharedTestPattern("p2", "r4")
        p3 = SharedTestPattern("p3", "r5")

        patterns = {
            p1.name: p1,
            p2.name: p2,
            p3.name: p3
        }

        monitor = SharedTestMonitor(patterns, {})

        self.assertEqual(len(monitor._patterns), len(patterns))
        for k, v in patterns.items():
            self.assertIn(k, monitor._patterns)
            self.assertNotEqual(monitor._patterns[k], v)            
            self.assertEqual(monitor._patterns[k].name, v.name)
            self.assertEqual(monitor._patterns[k].recipe, v.recipe)

        got_patterns = monitor.get_patterns()
        
        self.assertEqual(len(got_patterns), len(patterns))
        for k, v in patterns.items():
            self.assertIn(k, got_patterns)
            self.assertNotEqual(got_patterns[k], v)
            self.assertEqual(got_patterns[k].name, v.name)
            self.assertEqual(got_patterns[k].recipe, v.recipe)

    # test we can add recipes
    def testBaseMonitorAddRecipe(self)->None:
        monitor = SharedTestMonitor({}, {})

        self.assertEqual(len(monitor._recipes), 0)

        r1 = SharedTestRecipe("r1", "")

        monitor.add_recipe(r1)

        self.assertEqual(len(monitor._recipes), 1)
        self.assertEqual(monitor._recipes[r1.name].name, r1.name)

    # test we can update recipes
    def testBaseMonitorUpdateRecipe(self)->None:
        starting_rec = "starting_rec"
        updated_rec = "update_rec"

        r1 = SharedTestRecipe("r1", starting_rec)

        monitor = SharedTestMonitor(
            {}, 
            {
                r1.name: r1
            }
        )

        self.assertEqual(len(monitor._recipes), 1)
        self.assertEqual(monitor._recipes[r1.name].name, r1.name)
        self.assertEqual(monitor._recipes[r1.name].recipe, r1.recipe)

        r1.recipe = updated_rec

        self.assertEqual(len(monitor._recipes), 1)
        self.assertEqual(monitor._recipes[r1.name].name, r1.name)
        self.assertEqual(monitor._recipes[r1.name].recipe, starting_rec)

        monitor.update_recipe(r1)

        self.assertEqual(len(monitor._recipes), 1)
        self.assertEqual(monitor._recipes[r1.name].name, r1.name)
        self.assertEqual(monitor._recipes[r1.name].recipe, r1.recipe)

    # test we can remove recipes
    def testBaseMonitorRemoveRecipe(self)->None:
        r1 = SharedTestRecipe("r1", "")

        monitor = SharedTestMonitor(
            {}, 
            {
                r1.name: r1
            }
        )

        self.assertEqual(len(monitor._recipes), 1)
        self.assertEqual(monitor._recipes[r1.name].name, r1.name)

        monitor.remove_recipe(r1)

        self.assertEqual(len(monitor._recipes), 0)

    # test we can retrieve recipes
    def testBaseMonitorGetRecipes(self)->None:
        r1 = SharedTestRecipe("r1", "")
        r2 = SharedTestRecipe("r2", "")
        r3 = SharedTestRecipe("r3", "")

        recipes = {
            r1.name: r1,
            r2.name: r2,
            r3.name: r3
        }

        monitor = SharedTestMonitor({}, recipes)

        self.assertEqual(len(monitor._recipes), len(recipes))
        for k, v in recipes.items():
            self.assertIn(k, monitor._recipes)
            self.assertNotEqual(monitor._recipes[k], v)
            self.assertEqual(monitor._recipes[k].name, v.name)
            self.assertEqual(monitor._recipes[k].recipe, v.recipe)

        got_recipes = monitor.get_recipes()
        
        self.assertEqual(len(got_recipes), len(recipes))
        for k, v in recipes.items():
            self.assertIn(k, got_recipes)
            self.assertNotEqual(got_recipes[k], v)
            self.assertEqual(got_recipes[k].name, v.name)
            self.assertEqual(got_recipes[k].recipe, v.recipe)

    # test we can recieve rules
    def testBaseMonitorGetRules(self)->None:
        p1 = SharedTestPattern("p1", "r1")
        p2 = SharedTestPattern("p2", "r2")
        p3 = SharedTestPattern("p3", "r3")
        r1 = SharedTestRecipe("r1", "")
        r2 = SharedTestRecipe("r2", "")
        r3 = SharedTestRecipe("r3", "")

        monitor = SharedTestMonitor(
            {
                p1.name: p1,
                p2.name: p2,
                p3.name: p3
            }, 
            {
                r1.name: r1,
                r2.name: r2,
                r3.name: r3
            }
        )

        self.assertEqual(len(monitor._rules), 3)

        retieved_rules = monitor.get_rules()

        self.assertEqual(len(retieved_rules), len(monitor._rules))
        for k, v in retieved_rules.items():
            self.assertIn(k, monitor._rules)
            self.assertNotEqual(monitor._rules[k], v)
            self.assertEqual(monitor._rules[k].name, v.name)
            self.assertNotEqual(monitor._rules[k].pattern, v.pattern)
            self.assertEqual(monitor._rules[k].pattern.name, v.pattern.name)
            self.assertEqual(monitor._rules[k].pattern.recipe, v.pattern.recipe)
            self.assertNotEqual(monitor._rules[k].recipe, v.recipe)
            self.assertEqual(monitor._rules[k].recipe.name, v.recipe.name)
            self.assertEqual(monitor._rules[k].recipe.recipe, v.recipe.recipe)

# TODO test for base functions
class BaseHandleTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test BaseHandler instantiation
    def testBaseHandler(self)->None:
        with self.assertRaises(TypeError):
            BaseHandler()

        class EmptyTestHandler(BaseHandler):
            pass

        with self.assertRaises(NotImplementedError):
            EmptyTestHandler()

        SharedTestHandler()


# TODO test for base functions
class BaseConductorTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test BaseConductor instantiation
    def testBaseConductor(self)->None:
        with self.assertRaises(TypeError):
            BaseConductor()

        class EmptyTestConductor(BaseConductor):
            pass

        with self.assertRaises(NotImplementedError):
            EmptyTestConductor()

        SharedTestConductor()
