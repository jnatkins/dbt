from typing import MutableMapping, Dict
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.files import (
    AnySourceFile, ParseFileType, parse_file_type_to_parser,
)
from dbt.logger import GLOBAL_LOGGER as logger


mssat_files = (
    ParseFileType.Model,
    ParseFileType.Seed,
    ParseFileType.Snapshot,
    ParseFileType.Analysis,
    ParseFileType.Test,
)


key_to_prefix = {
    'models': 'model',
    'seeds': 'seed',
    'snapshots': 'snapshot',
    'analyses': 'analysis',
}


parse_file_type_to_key = {
    ParseFileType.Model: 'models',
    ParseFileType.Seed: 'seeds',
    ParseFileType.Snapshot: 'snapshots',
    ParseFileType.Analysis: 'analyses',
}


# Partial parsing. Create a diff of files from saved manifest and current
# files and produce a project_parser_file dictionary to drive parsing of
# only the necessary changes.
# Will produce a 'skip_parsing' method, and a project_parser_file dictionary
class PartialParsing:
    def __init__(self, saved_manifest: Manifest, new_files: MutableMapping[str, AnySourceFile]):
        self.saved_manifest = saved_manifest
        self.new_files = new_files
        self.project_parser_files: Dict = {}
        self.saved_files = self.saved_manifest.files
        self.project_parser_files = {}
        self.deleted_manifest = Manifest()
        self.build_file_diff()

    def skip_parsing(self):
        return (
            not self.file_diff['deleted'] and
            not self.file_diff['added'] and
            not self.file_diff['changed'] and
            not self.file_diff['changed_schema_files'] and
            not self.file_diff['deleted_schema_files']
        )

    # Compare the previously saved manifest files and the just-loaded manifest
    # files to see if anything changed
    def build_file_diff(self):
        saved_file_ids = set(self.saved_files.keys())
        new_file_ids = set(self.new_files.keys())
        deleted_all_files = saved_file_ids.difference(new_file_ids)
        added = new_file_ids.difference(saved_file_ids)
        common = saved_file_ids.intersection(new_file_ids)

        # separate out deleted schema files
        deleted_schema_files = []
        deleted = []
        for file_id in deleted_all_files:
            if self.saved_files[file_id].parse_file_type == ParseFileType.Schema:
                deleted_schema_files.append(file_id)
            else:
                deleted.append(file_id)

        changed = []
        changed_schema_files = []
        unchanged = []
        for file_id in common:
            if self.saved_files[file_id].checksum == self.new_files[file_id].checksum:
                unchanged.append(file_id)
            else:
                # separate out changed schema files
                if self.saved_files[file_id].parse_file_type == ParseFileType.Schema:
                    sf = self.saved_files[file_id]
                    if type(sf).__name__ != 'SchemaSourceFile':
                        raise Exception(f"Serialization failure for {file_id}")
                    changed_schema_files.append(file_id)
                else:
                    changed.append(file_id)
        file_diff = {
            "deleted": deleted,
            "deleted_schema_files": deleted_schema_files,
            "added": added,
            "changed": changed,
            "changed_schema_files": changed_schema_files,
            "unchanged": unchanged,
        }
        logger.info(f"Partial parsing enabled: "
                    f"{len(deleted) + len(deleted_schema_files)} files deleted, "
                    f"{len(added)} files added, "
                    f"{len(changed) + len(changed_schema_files)} files changed.")
        self.file_diff = file_diff

    # generate the list of files that need parsing
    # uses self.manifest.files generated by 'read_files'
    def get_parsing_files(self):
        if self.skip_parsing():
            return {}
        # Need to add new files first, because changes in schema files
        # might refer to them
        for file_id in self.file_diff['added']:
            self.add_to_saved(file_id)
        # Need to process schema files next, because the dictionaries
        # need to be in place for handling SQL file changes
        for file_id in self.file_diff['changed_schema_files']:
            self.change_schema_file(file_id)
        for file_id in self.file_diff['deleted_schema_files']:
            self.delete_schema_file(file_id)
        for file_id in self.file_diff['deleted']:
            self.delete_from_saved(file_id)
        for file_id in self.file_diff['changed']:
            self.update_in_saved(file_id)
        return self.project_parser_files

    # new files are easy, just add them to parse list
    def add_to_saved(self, file_id):
        # add file object to saved manifest.files
        source_file = self.new_files[file_id]
        if source_file.parse_file_type == ParseFileType.Schema:
            source_file.pp_dict = source_file.dict_from_yaml.copy()
        self.saved_files[file_id] = source_file
        # update pp_files to parse
        self.add_to_pp_files(source_file)
        logger.debug(f"Partial parsing: added file: {file_id}")

    # This handles all non-schema files
    def delete_from_saved(self, file_id):
        # Look at all things touched by file, remove those
        # nodes, and update pp_files to parse unless the
        # file creating those nodes has also been deleted
        saved_source_file = self.saved_files[file_id]

        # SQL file: models, seeds, snapshots, analyses, tests: SQL files, except
        # macros/tests
        if saved_source_file.parse_file_type in mssat_files:
            self.delete_mssat_file(saved_source_file)

        # macros
        if saved_source_file.parse_file_type == ParseFileType.Macro:
            self.delete_macro_file(saved_source_file)

        # docs
        if saved_source_file.parse_file_type == ParseFileType.Documentation:
            self.delete_doc_file(saved_source_file)

        self.deleted_manifest.files[file_id] = self.saved_manifest.files.pop(file_id)
        logger.debug(f"Partial parsing: deleted file: {file_id}")

    # schema files already updated
    def update_in_saved(self, file_id):
        new_source_file = self.new_files[file_id]
        old_source_file = self.saved_files[file_id]

        if new_source_file.parse_file_type in mssat_files:
            self.update_mssat_in_saved(new_source_file, old_source_file)
        elif new_source_file.parse_file_type == ParseFileType.Macro:
            self.update_macro_in_saved(new_source_file, old_source_file)
        elif new_source_file.parse_file_type == ParseFileType.Documentation:
            self.update_doc_in_saved(new_source_file, old_source_file)
        else:
            raise Exception(f"Invalid parse_file_type in source_file {file_id}")
        logger.debug(f"Partial parsing: updated file: {file_id}")

    # This is models, seeds, snapshots, analyses, tests.
    # Models, seeds, snapshots: patches and tests
    # analyses: patches, no tests
    # tests: not touched by schema files (no patches, no tests)
    # Updated schema files should have been processed already.
    def update_mssat_in_saved(self, new_source_file, old_source_file):

        # These files only have one node.
        unique_id = old_source_file.nodes[0]

        # replace source_file in saved and add to parsing list
        file_id = new_source_file.file_id
        self.deleted_manifest.files[file_id] = old_source_file
        self.saved_files[file_id] = new_source_file
        self.add_to_pp_files(new_source_file)
        self.delete_node_in_saved(new_source_file, unique_id)

    def delete_node_in_saved(self, source_file, unique_id):
        # delete node in saved
        node = self.saved_manifest.nodes.pop(unique_id)
        self.deleted_manifest.nodes[unique_id] = node

        # look at patch_path in model node to see if we need
        # to reapply a patch from a schema_file.
        if node.patch_path:
            file_id = node.patch_path
            # it might be changed...  then what?
            if file_id not in self.file_diff['deleted']:
                # schema_files should already be updated
                schema_file = self.saved_files[file_id]
                dict_key = parse_file_type_to_key[source_file.parse_file_type]
                # look for a matching list dictionary
                for elem in schema_file.dict_from_yaml[dict_key]:
                    if elem['name'] == node.name:
                        elem_patch = elem
                if elem_patch:
                    self.delete_schema_mssa_links(schema_file, dict_key, elem_patch)
                    self.merge_patch(schema_file, dict_key, elem_patch)
                    self.add_to_pp_files(schema_file)
                    if unique_id in schema_file.node_patches:
                        schema_file.node_patches.remove(unique_id)

    def update_macro_in_saved(self, new_source_file, old_source_file):
        self.handle_macro_file_links(old_source_file)
        file_id = new_source_file.file_id
        self.saved_files[file_id] = new_source_file
        self.add_to_pp_files(new_source_file)

    def update_doc_in_saved(self, new_source_file, old_source_file):
        self.saved_files[new_source_file.file_id] = new_source_file
        self.add_to_pp_files(new_source_file)
        logger.warning("Partial parse is enabled and a doc file was updated, "
                       "but references to the doc will not be rebuilt. Please rebuild "
                       "without partial parsing.")

    def change_schema_file(self, file_id):
        saved_schema_file = self.saved_files[file_id]
        new_schema_file = self.new_files[file_id]
        saved_yaml_dict = saved_schema_file.dict_from_yaml
        new_yaml_dict = new_schema_file.dict_from_yaml
        saved_schema_file.pp_dict = {"version": saved_yaml_dict['version']}
        self.handle_schema_file_changes(saved_schema_file, saved_yaml_dict, new_yaml_dict)

        # copy from new schema_file to saved_schema_file to preserve references
        # that weren't removed
        saved_schema_file.contents = new_schema_file.contents
        saved_schema_file.checksum = new_schema_file.checksum
        saved_schema_file.dfy = new_schema_file.dfy
        # schedule parsing
        self.add_to_pp_files(saved_schema_file)
        # schema_file pp_dict should have been generated already
        logger.debug(f"Partial parsing: update schema file: {file_id}")

    # This is a variation on changed schema files
    def delete_schema_file(self, file_id):
        saved_schema_file = self.saved_files[file_id]
        saved_yaml_dict = saved_schema_file.dict_from_yaml
        new_yaml_dict = {}
        self.handle_schema_file_changes(saved_schema_file, saved_yaml_dict, new_yaml_dict)
        self.deleted_manifest.files[file_id] = self.saved_manifest.files.pop(file_id)

    # For each key in a schema file dictionary, process the changed, deleted, and added
    # elemnts for the key lists
    def handle_schema_file_changes(self, schema_file, saved_yaml_dict, new_yaml_dict):
        # loop through comparing previous dict_from_yaml with current dict_from_yaml
        # Need to do the deleted/added/changed thing, just like the files lists

        # models, seeds, snapshots, analyses
        for dict_key in ['models', 'seeds', 'snapshots', 'analyses']:
            key_diff = self.get_diff_for(dict_key, saved_yaml_dict, new_yaml_dict)
            if key_diff['changed']:
                for elem in key_diff['changed']:
                    self.delete_schema_mssa_links(schema_file, dict_key, elem)
                    self.merge_patch(schema_file, dict_key, elem)
                    if 'enabled' in elem and elem['enabled'] = 'false':
                        # TODO: schedule_references_nodes_for_parsing?
            if key_diff['deleted']:
                for elem in key_diff['deleted']:
                    self.delete_schema_mssa_links(schema_file, dict_key, elem)
                    # patches will go away when new is copied to saved
            if key_diff['added']:
                for elem in key_diff['added']:
                    self.merge_patch(schema_file, dict_key, elem)

        # sources
        source_diff = self.get_diff_for('sources', saved_yaml_dict, new_yaml_dict)
        if source_diff['changed']:
            for source in source_diff['changed']:
                if 'override' in source:
                    raise Exception(f"Partial parsing does not handle changed "
                                    f"source overrides: {schema_file.file_id}")
                self.delete_schema_source(schema_file, source)
                self.merge_patch(schema_file, 'sources', source)
        if source_diff['deleted']:
            for source in source_diff['deleted']:
                if 'override' in source:
                    raise Exception(f"Partial parsing does not handle deleted "
                                    f"source overrides: {schema_file.file_id}")
                self.delete_schema_source(schema_file, source)
        if source_diff['added']:
            for source in source_diff['added']:
                if 'override' in source:
                    raise Exception(f"Partial parsing does not handle new "
                                    f"source overrides: {schema_file.file_id}")
                self.merge_patch(schema_file, 'sources', source)

        # macros
        macro_diff = self.get_diff_for('macros', saved_yaml_dict, new_yaml_dict)
        if macro_diff['changed']:
            for macro in macro_diff['changed']:
                self.delete_schema_macro_patch(schema_file, macro)
                self.merge_patch(schema_file, 'macros', elem)
        if macro_diff['deleted']:
            for macro in macro_diff['deleted']:
                self.delete_schema_macro_patch(schema_file, macro)
        if macro_diff['added']:
            for elem in macro_diff['added']:
                self.merge_patch(schema_file, 'macros', elem)

        # exposures
        exposure_diff = self.get_diff_for('exposures', saved_yaml_dict, new_yaml_dict)
        if exposure_diff['changed']:
            for exposure in exposure_diff['changed']:
                self.delete_schema_exposure(schema_file, exposure)
                self.merge_patch(schema_file, 'exposures', exposure)
        if exposure_diff['deleted']:
            for exposure in exposure_diff['deleted']:
                self.delete_schema_exposure(schema_file, exposure)
        if exposure_diff['added']:
            for exposure in exposure_diff['added']:
                self.merge_patch(schema_file, 'exposures', exposure)

    # Take a "section" of the schema file yaml dictionary from saved and new schema files
    # and determine which parts have changed
    def get_diff_for(self, key, saved_yaml_dict, new_yaml_dict):
        if key in saved_yaml_dict or key in new_yaml_dict:
            saved_elements = saved_yaml_dict[key] if key in saved_yaml_dict else []
            new_elements = new_yaml_dict[key] if key in new_yaml_dict else []
        else:
            return {'deleted': [], 'added': [], 'changed': []}
        # for each set of keys, need to create a dictionary of names pointing to entry
        saved_elements_by_name = {}
        new_elements_by_name = {}
        # sources have two part names?
        for element in saved_elements:
            saved_elements_by_name[element['name']] = element
        for element in new_elements:
            new_elements_by_name[element['name']] = element

        # now determine which elements, by name, are added, deleted or changed
        saved_element_names = set(saved_elements_by_name.keys())
        new_element_names = set(new_elements_by_name.keys())
        deleted = saved_element_names.difference(new_element_names)
        added = new_element_names.difference(saved_element_names)
        common = saved_element_names.intersection(new_element_names)
        changed = []
        for element_name in common:
            if saved_elements_by_name[element_name] != new_elements_by_name[element_name]:
                changed.append(element_name)

        # make lists of yaml elements to return as diffs
        deleted_elements = [saved_elements_by_name[name].copy() for name in deleted]
        added_elements = [new_elements_by_name[name].copy() for name in added]
        changed_elements = [new_elements_by_name[name].copy() for name in changed]

        diff = {
            "deleted": deleted_elements,
            "added": added_elements,
            "changed": changed_elements,
        }
        return diff

    # Add the file to the project parser dictionaries to schedule parsing
    def add_to_pp_files(self, source_file):
        file_id = source_file.file_id
        parser_name = parse_file_type_to_parser[source_file.parse_file_type]
        project_name = source_file.project_name
        if not parser_name or not project_name:
            raise Exception(f"Did not find parse_file_type or project_name "
                            f"in SourceFile for {source_file.file_id}")
        if project_name not in self.project_parser_files:
            self.project_parser_files[project_name] = {}
        if parser_name not in self.project_parser_files[project_name]:
            self.project_parser_files[project_name][parser_name] = []
        if (file_id not in self.project_parser_files[project_name][parser_name] and
                file_id not in self.file_diff['deleted']):
            self.project_parser_files[project_name][parser_name].append(file_id)

    # Merge a patch file into the pp_dict in a schema file
    def merge_patch(self, schema_file, key, patch):
        if not schema_file.pp_dict:
            schema_file.pp_dict = {"version": schema_file.dict_from_yaml['version']}
        pp_dict = schema_file.pp_dict
        if key not in pp_dict:
            pp_dict[key] = [patch]
        else:
            # check that this patch hasn't already been saved
            found = False
            for elem in pp_dict[key]:
                if elem['name'] == patch['name']:
                    found = True
            if not found:
                pp_dict[key].append(patch)

    # For model, seed, snapshot, analysis schema dictionary keys,
    # delete the patches and tests from the patch
    def delete_schema_mssa_links(self, schema_file, dict_key, elem):
        # find elem node unique_id in node_patches

        prefix = key_to_prefix[dict_key]
        elem_unique_id = ''
        for unique_id in schema_file.node_patches:
            if not unique_id.startswith(prefix):
                continue
            parts = unique_id.split('.')
            elem_name = parts[-1]
            if elem_name == elem['name']:
                elem_unique_id = unique_id
                break

        # remove elem node and remove unique_id from node_patches
        if elem_unique_id:
            # might have been already removed
            if elem_unique_id in self.saved_manifest.nodes:
                node = self.saved_manifest.nodes.pop(elem_unique_id)
                self.deleted_manifest.nodes[elem_unique_id] = node
                # need to add the node source_file to pp_files
                file_id = node.file_id()
                # need to copy new file to saved files in order to get content
                if self.new_files[file_id]:
                    self.saved_files[file_id] = self.new_files[file_id]
                if self.saved_files[file_id]:
                    source_file = self.saved_files[file_id]
                    self.add_to_pp_files(source_file)
            # TODO: should removing patches be here or with the 'merge_patch' code?
            # remove from patches
            schema_file.node_patches.remove(elem_unique_id)

        # for models, seeds, snapshots (not analyses)
        if dict_key in ['models', 'seeds', 'snapshots']:
            # find related tests and remove them
            tests = self.get_tests_for(schema_file, elem['name'])
            for test_unique_id in tests:
                node = self.saved_manifest.nodes.pop(test_unique_id)
                self.deleted_manifest.nodes[test_unique_id] = node
                schema_file.tests.remove(test_unique_id)

    # Create a pp_test_index in the schema file if it doesn't exist
    # and look for test names related to this yaml dict element name
    def get_tests_for(self, schema_file, node_name):
        if not schema_file.pp_test_index:
            pp_test_index = {}
            for test_unique_id in schema_file.tests:
                test_node = self.saved_manifest.nodes[test_unique_id]
                tested_node_id = test_node.depends_on.nodes[0]
                parts = tested_node_id.split('.')
                elem_name = parts[-1]
                if elem_name in pp_test_index:
                    pp_test_index[elem_name].append(test_unique_id)
                else:
                    pp_test_index[elem_name] = [test_unique_id]
            schema_file.pp_test_index = pp_test_index
        if node_name in schema_file.pp_test_index:
            return schema_file.pp_test_index[node_name]
        return []

    def delete_mssat_file(self, source_file):
        # nodes [unique_ids] -- SQL files
        # There should always be a node for a SQL file
        if not source_file.nodes:
            raise Exception(f"No nodes found for source file {source_file.file_id}")
        # There is generally only 1 node for SQL files, except for macros
        for unique_id in source_file.nodes:
            self.delete_node_in_saved(source_file, unique_id)
            self.schedule_nodes_for_parsing(source_file, unique_id)

    def delete_macro_file(self, source_file):
        self.handle_macro_file_links(source_file)
        file_id = source_file.file_id
        self.deleted_manifest.files[file_id] = self.saved_files.pop(file_id)

    def handle_macro_file_links(self, source_file):
        # remove the macros in the 'macros' dictionary
        for unique_id in source_file.macros:
            self.deleted_manifest.macros[unique_id] = self.saved_manifest.macros.pop(unique_id)
            # loop through all macros, finding references to this macro: macro.depends_on.macros
            for macro in self.saved_manifest.macros.values():
                for macro_unique_id in macro.depends_on.macros:
                    if (macro_unique_id == unique_id and
                            macro_unique_id in self.saved_manifest.macros):
                        # schedule file for parsing
                        dep_file_id = macro.file_id()
                        if dep_file_id in self.saved_files:
                            source_file = self.saved_files[dep_file_id]
                            self.add_to_pp_files(source_file)
            # loop through all nodes, finding references to this macro: node.depends_on.macros
            for node in self.saved_manifest.nodes.values():
                for macro_unique_id in node.depends_on.macros:
                    if (macro_unique_id == unique_id and
                            macro_unique_id in self.saved_manifest.macros):
                        # schedule file for parsing
                        dep_file_id = node.file_id()
                        if dep_file_id in self.saved_files:
                            source_file = self.saved_files[dep_file_id]
                            self.add_to_pp_files(source_file)

    def delete_doc_file(self, source_file):
        file_id = source_file.file_id
        # remove the nodes in the 'docs' dictionary
        for unique_id in source_file.docs:
            self.deleted_manifest.docs[unique_id] = self.saved_manifest.docs.pop(unique_id)
            logger.warning(f"Doc file {file_id} was deleted, but partial parsing cannot update "
                           f"doc references. Please rebuild to regenerate docs.")
        # remove the file from the saved_files
        self.deleted_manifest.files[file_id] = self.saved_files.pop(file_id)

    def delete_schema_source(self, schema_file, source_dict):
        # both patches, tests, and source nodes
        source_name = source_dict['name']
        # There may be multiple sources for each source dict, since
        # there will be a separate source node for each table.
        # ParsedSourceDefinition name = table name, dict name is source_name
        for unique_id in schema_file.sources:
            if unique_id in self.saved_manifest.sources:
                source = self.saved_manifest.sources[unique_id]
                if source.source_name == source_name:
                    source = self.saved_manifest.exposures.pop(unique_id)
                    self.deleted_manifest.sources[unique_id] = source
                    logger.debug(f"Partial parsing: deleted source {unique_id}")

    def delete_schema_macro_patch(self, schema_file, macro):
        # This is just macro patches that need to be reapplied
        for unique_id in schema_file.macro_patches:
            parts = unique_id.split('.')
            macro_name = parts[-1]
            if macro_name == macro['name']:
                macro_unique_id = unique_id
                break
        if macro_unique_id and macro_unique_id in self.saved_manifest.macros:
            macro = self.saved_manifest.macros.pop(macro_unique_id)
            self.deleted_manifest.macros[macro_unique_id] = macro
            macro_file_id = macro.file_id()
            self.add_to_pp_files(self.saved_files[macro_file_id])
        if macro_unique_id in schema_file.macro_patches:
            schema_file.macro_patches.remove(macro_unique_id)

    # exposures are created only from schema files, so just delete
    # the exposure.
    def delete_schema_exposure(self, schema_file, exposure_dict):
        exposure_name = exposure_dict['name']
        for unique_id in schema_file.exposures:
            exposure = self.saved_manifest.exposures[unique_id]
            if unique_id in self.saved_manifest.exposures:
                if exposure.name == exposure_name:
                    self.deleted_manifest.exposures[unique_id] = \
                        self.saved_manifest.exposures.pop(unique_id)
                    logger.debug(f"Partial parsing: deleted exposure {unique_id}")

    def schedule_referenced_nodes_for_parsing(source_file, unique_id):
        # Look at "children", i.e. nodes that reference this node
        for unique_id in self.saved_manifest.child_map[unique_id]:
            if unique_id in self.saved_manifest.nodes:
                node = self.saved_manifest.nodes[unique_id]
                # TODO: now do something with the node. Need a bit of refactoring...
