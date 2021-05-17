from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile
import os

from dbt.task.test import TestTask
from dbt.exceptions import CompilationException
from dbt.contracts.results import TestStatus

class TestSchemaTestContext(DBTIntegrationTest):
    @property
    def schema(self):
        return "schema_tests_008"

    @property
    def models(self):
        return "test-context-models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ["test-context-macros"],
            "vars": {
                'local_utils_dispatch_list': ['local_utils']
            }
        }

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'local': 'local_utils'
                }
            ]
        }

    @use_profile('postgres')
    def test_postgres_test_context_tests(self):
        # This test tests the the TestContext and TestMacroNamespace
        # are working correctly
        self.run_dbt(['deps'])
        results = self.run_dbt(strict=False)
        self.assertEqual(len(results), 3)

        run_result = self.run_dbt(['test'], expect_pass=False)
        results = run_result.results
        results = sorted(results, key=lambda r: r.node.name)
        self.assertEqual(len(results), 4)
        # call_pkg_macro_model_c_
        self.assertEqual(results[0].status, TestStatus.Fail)
        # pkg_and_dispatch_model_c_
        self.assertEqual(results[1].status, TestStatus.Fail)
        # type_one_model_a_
        self.assertEqual(results[2].status, TestStatus.Fail)
        self.assertRegex(results[2].node.compiled_sql, r'union all')
        # type_two_model_a_
        self.assertEqual(results[3].status, TestStatus.Fail)
        self.assertEqual(results[3].node.config.severity, 'WARN')
