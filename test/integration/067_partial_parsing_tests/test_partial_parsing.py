from dbt.exceptions import CompilationException
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.files import ParseFileType
from test.integration.base import DBTIntegrationTest, use_profile, normalize
import shutil
import os

def get_manifest():
    path = './target/partial_parse.msgpack'
    if os.path.exists(path):
        with open(path, 'rb') as fp:
            manifest_mp = fp.read()
        manifest: Manifest = Manifest.from_msgpack(manifest_mp)
        return manifest
    else:
        return None

class TestModels(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_067"

    @property
    def models(self):
        return "models"


    @use_profile('postgres')
    def test_postgres_pp_models(self):
        # initial run
        self.run_dbt(['clean'])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)

        # add a model file
        shutil.copyfile('extra-files/model_two.sql', 'models/model_two.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 2)

        # add a schema file
        shutil.copyfile('extra-files/models-schema1.yml', 'models/schema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 2)
        manifest = get_manifest()
        self.assertIn('model.test.model_one', manifest.nodes)
        model_one_node = manifest.nodes['model.test.model_one']
        self.assertEqual(model_one_node.description, 'The first model')
        self.assertEqual(model_one_node.patch_path, 'test://' + normalize('models/schema.yml'))

        # add a model and a schema file (with a test) at the same time
        shutil.copyfile('extra-files/models-schema2.yml', 'models/schema.yml')
        shutil.copyfile('extra-files/model_three.sql', 'models/model_three.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)
        manifest = get_manifest()
        self.assertEqual(len(manifest.files), 33)
        model_3_file_id = 'test://' + normalize('models/model_three.sql')
        self.assertIn(model_3_file_id, manifest.files)
        model_three_file = manifest.files[model_3_file_id]
        self.assertEqual(model_three_file.parse_file_type, ParseFileType.Model)
        self.assertEqual(type(model_three_file).__name__, 'SourceFile')
        model_three_node = manifest.nodes[model_three_file.nodes[0]]
        schema_file_id = 'test://' + normalize('models/schema.yml')
        self.assertEqual(model_three_node.patch_path, schema_file_id)
        self.assertEqual(model_three_node.description, 'The third model')
        schema_file = manifest.files[schema_file_id]
        self.assertEqual(type(schema_file).__name__, 'SchemaSourceFile')
        self.assertEqual(len(schema_file.tests), 1)

        # go back to previous version of schema file, removing patch and test for model three
        shutil.copyfile('extra-files/models-schema1.yml', 'models/schema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # remove schema file, still have 3 models
        os.remove(normalize('models/schema.yml'))
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)
        manifest = get_manifest()
        schema_file_id = 'test://' + normalize('models/schema.yml')
        self.assertNotIn(schema_file_id, manifest.files)
        self.assertEqual(len(manifest.files), 32)

        # Put schema file back and remove a model
        # referred to in schema file
        shutil.copyfile('extra-files/models-schema2.yml', 'models/schema.yml')
        os.remove(normalize('models/model_three.sql'))
        with self.assertRaises(CompilationException):
            results = self.run_dbt(["--partial-parse", "run"])

        # Put model back again
        shutil.copyfile('extra-files/model_three.sql', 'models/model_three.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

    def tearDown(self):
        if os.path.exists(normalize('models/model_two.sql')):
            os.remove(normalize('models/model_two.sql'))
        if os.path.exists(normalize('models/model_three.sql')):
            os.remove(normalize('models/model_three.sql'))
        if os.path.exists(normalize('models/schema.yml')):
            os.remove(normalize('models/schema.yml'))


class TestSources(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_067"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        cfg = {
            'config-version': 2,
            'data-paths': ['seed'],
            'seeds': {
                'quote_columns': False,
            },
        }
        return cfg

    def tearDown(self):
        if os.path.exists(normalize('models/sources.yml')):
            os.remove(normalize('models/sources.yml'))
        if os.path.exists(normalize('seed/raw_customers.csv')):
            os.remove(normalize('seed/raw_customers.csv'))


    @use_profile('postgres')
    def test_postgres_pp_sources(self):
        # initial run
        shutil.copyfile('extra-files/raw_customers.csv', 'seed/raw_customers.csv')
        self.run_dbt(['clean'])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)

        # create a seed file, parse and run it
        self.run_dbt(['seed'])
        manifest = get_manifest()
        seed_file_id = 'test://' + normalize('seed/raw_customers.csv')
        self.assertIn(seed_file_id, manifest.files)

        # add a schema files with a source referring to raw_customers
        shutil.copyfile('extra-files/schema-sources1.yml', 'models/sources.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.sources), 1)
        file_id = 'test://' + normalize('models/sources.yml')
        self.assertIn(file_id, manifest.files)

        # remove sources schema file
        os.remove(normalize('models/sources.yml'))
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.sources), 0)


class TestPartialParsingDependency(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_067"

    @property
    def models(self):
        return "models"

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'local': 'local_dependency'
                }
            ]
        }

    @use_profile("postgres")
    def test_postgres_duplicate_model_enabled_across_packages(self):
        self.run_dbt(["deps"])
#       self.run_dbt(["run"])


