"""Structural checks for prospective contracts, never product acceptance tests."""
import hashlib
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


def read(name):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON key: " + key)
            result[key] = value
        return result
    return json.loads((ROOT/name).read_text(), object_pairs_hook=unique)


def keyed(items):
    result = {item["id"]: item for item in items}
    if len(result) != len(items):
        raise ValueError("Repeated contract identity")
    return result


def acyclic(nodes, dependencies):
    done, active = set(), set()
    def visit(name):
        if name in active:
            raise ValueError("Cyclic dependency: " + name)
        if name in done:
            return
        if name not in nodes:
            raise ValueError("Undeclared dependency: " + name)
        active.add(name)
        for parent in dependencies(nodes[name]):
            visit(parent)
        active.remove(name)
        done.add(name)
    for name in nodes:
        visit(name)


class ProspectiveProductContractTests(unittest.TestCase):
    def test_m1_inventory_is_complete_as_a_mapping_but_not_an_execution(self):
        data = read("library-m1-acceptance-inventory-v1.json")
        rows, gates = keyed(data["requirements"]), keyed(data["gate_definitions"])
        mappings = {item["requirement_id"]: item for item in data["requirement_gate_mappings"]}
        self.assertEqual(len(mappings), len(data["requirement_gate_mappings"]))
        self.assertEqual(set(mappings), set(rows))
        self.assertEqual(data["scope"]["requirement_count"], len(rows))
        self.assertFalse(data["scope"]["whole_m1_qualified"])
        self.assertFalse(data["scope"]["whole_project_acceptance"])
        self.assertEqual(data["status"], "prospective_unqualified")
        for name, row in rows.items():
            self.assertIs(row["mandatory"], True)
            self.assertEqual(row["current_product_verdict"], "not_evaluated")
            self.assertIs(row["definition_evidence_only"], True)
            self.assertEqual(row["acceptance_observations"], [])
            declared = mappings[name]["required_gate_ids"]
            self.assertTrue(declared)
            self.assertEqual(len(declared), len(set(declared)))
            self.assertEqual(declared, row["required_gate_ids"])
            self.assertLessEqual(set(declared), set(gates))
            prerequisites = mappings[name]["required_prerequisite_gate_ids"]
            self.assertEqual(prerequisites, row["required_prerequisite_gate_ids"])
            self.assertEqual(len(prerequisites), len(set(prerequisites)))
            self.assertFalse(set(declared) & set(prerequisites))
            self.assertLessEqual(set(prerequisites), set(gates))
            self.assertTrue(all(gates[key]["consumer_role"] == "qualification_prerequisite"
                                for key in prerequisites))
            expected_role = "product_registry" if row["acceptance_role"] == "product_requirement" else "qualification_prerequisite"
            self.assertTrue(all(gates[key]["consumer_role"] == expected_role for key in declared))
            self.assertEqual(set(declared) | set(prerequisites),
                             {key for key, gate in gates.items() if name in gate["requirement_ids"]})
        self.assertEqual(set(rows), {name for gate in gates.values() for name in gate["requirement_ids"]})
        for gate in gates.values():
            self.assertTrue(gate["requirement_ids"])
            self.assertEqual(len(gate["requirement_ids"]), len(set(gate["requirement_ids"])))
            self.assertEqual(gate["status"], "planned_not_qualified")
            self.assertIs(gate["check_definitions_frozen"], False)
            self.assertEqual(gate["observations"], [])

    def test_m1_product_and_qualification_subjects_are_not_interchangeable(self):
        data = read("library-m1-acceptance-inventory-v1.json")
        rows, gates = keyed(data["requirements"]), keyed(data["gate_definitions"])
        boundary = data["consumer_boundary"]
        product = set(boundary["product_requirement_ids"])
        prerequisite = set(boundary["prerequisite_requirement_ids"])
        self.assertFalse(product & prerequisite)
        self.assertEqual(product | prerequisite, set(rows))
        product_gates, prerequisite_gates = set(boundary["product_gate_ids"]), set(boundary["prerequisite_gate_ids"])
        self.assertFalse(product_gates & prerequisite_gates)
        self.assertEqual(product_gates | prerequisite_gates, set(gates))
        for name in product:
            self.assertEqual(rows[name]["acceptance_role"], "product_requirement")
            self.assertEqual(rows[name]["binding_scope"], "same_final_product_subject")
        for name in product_gates:
            self.assertEqual(gates[name]["consumer_role"], "product_registry")
            self.assertEqual(gates[name]["binding_scope"], "same_final_product_subject")
        for name in prerequisite_gates:
            self.assertEqual(gates[name]["consumer_role"], "qualification_prerequisite")
            self.assertEqual(gates[name]["binding_scope"], "declared_qualification_context")
        self.assertEqual(boundary["status"], "prospective_not_directly_consumable_by_registry")

    def test_m1_definition_references_and_gap_identities_resolve(self):
        data = read("library-m1-acceptance-inventory-v1.json")
        rows, evidence = keyed(data["requirements"]), keyed(data["evidence_catalog"])
        all_gaps = []
        for row in rows.values():
            self.assertLessEqual({item["evidence_id"] for item in row["existing"]}, set(evidence))
            gaps = [item["id"] for item in row["coverage_gaps"]]
            self.assertEqual(gaps, row["coverage_gap_ids"])
            all_gaps.extend(gaps)
        self.assertEqual(len(all_gaps), len(set(all_gaps)))
        for record in evidence.values():
            self.assertEqual(record["execution_receipts_audited"], [])
            self.assertEqual(record["candidate_execution_verdict"], "not_evaluated")
        clauses = keyed(data["clause_index"])
        self.assertEqual(set(rows), {name for clause in clauses.values() for name in clause["requirement_ids"]})
        sources = {item["path"]: item["sha256"] for item in data["provenance"]["source_files"]}
        for name, expected in sources.items():
            self.assertFalse(Path(name).is_absolute())
            self.assertNotIn("..", Path(name).parts)
            self.assertEqual(hashlib.sha256((ROOT/name).read_bytes()).hexdigest(), expected, name)
        references = [ref for row in rows.values() for ref in row["spec"]]
        references += [clause["spec"] for clause in clauses.values()]
        references += [ref for record in evidence.values() for ref in record["locations"]]
        for ref in references:
            self.assertIn(ref["path"], sources)
            self.assertIs(type(ref["start"]), int)
            self.assertIs(type(ref["end"]), int)
            self.assertGreaterEqual(ref["start"], 1)
            self.assertGreaterEqual(ref["end"], ref["start"])
            self.assertLessEqual(ref["end"], len((ROOT/ref["path"]).read_text().splitlines()))

    def test_cumulative_contract_mandatory_requirements_and_dependencies_are_closed(self):
        data = read("library-cumulative-product-v1.json")
        rows = keyed(data["requirements"])
        declared = data["acceptance_boundary"]["mandatory_requirement_ids"]
        self.assertEqual(list(rows), declared)
        self.assertEqual(set(item["milestone"] for item in rows.values()), {"M2", "M3", "M4"})
        self.assertEqual(data["status"], "prospective-unqualified")
        acyclic(rows, lambda row: row["depends_on"])
        for row in rows.values():
            self.assertEqual(set(row), set(data["registry_rules"]["requirement_record_keys"]))
            self.assertIs(row["mandatory"], True)
            self.assertEqual(row["status"], "prospective-unqualified")
            self.assertTrue(row["clauses"])
            self.assertTrue(row["evidence_lanes"])
            self.assertLessEqual(set(row["evidence_lanes"]), set(data["registry_rules"]["evidence_lanes"]))
            self.assertTrue(row["packages"])
            self.assertLessEqual(set(row["packages"]), {"catalog", "ingestion", "query", "clients"})
            for dependency in row["depends_on"]:
                self.assertLessEqual(rows[dependency]["milestone"], row["milestone"])
        for name, expected in data["inherited_contract"]["source_sha256"].items():
            self.assertEqual(hashlib.sha256((ROOT/name).read_bytes()).hexdigest(), expected, name)
        policy = data["freeze_policy"]
        self.assertIs(policy["no_execution_claim"], True)
        self.assertIs(policy["no_private_cases"], True)
        self.assertIs(policy["no_new_spending_authority"], True)
        self.assertTrue(policy["qualification_missing"])

    def test_feature_graph_does_not_omit_requirements_or_invent_parallel_slots(self):
        data = read("library-cumulative-product-v1.json")
        rows, graph = keyed(data["requirements"]), data["dependency_graph"]
        slots = keyed(graph["slots"])
        acyclic(slots, lambda slot: slot["depends_on_slots"])
        self.assertEqual(set(rows), {name for slot in slots.values() for name in slot["requirements"]})
        ready = [name for name, slot in slots.items() if not slot["depends_on_slots"]]
        self.assertEqual(ready, graph["expected_initial_ready_slots"])
        for slot in slots.values():
            self.assertEqual(set(slot), set(data["registry_rules"]["slot_record_keys"]))
            self.assertTrue(slot["requirements"])
            for name in slot["requirements"]:
                self.assertEqual(rows[name]["milestone"], slot["milestone"])
        # Exhaustive reachable frontiers of this small frozen feature DAG. A
        # candidate alternative never contributes another feature node here.
        pending, seen, maximum = [frozenset()], set(), 0
        while pending:
            completed = pending.pop()
            if completed in seen:
                continue
            seen.add(completed)
            ready = {name for name, slot in slots.items() if name not in completed
                     and set(slot["depends_on_slots"]) <= completed}
            maximum = max(maximum, len(ready))
            pending.extend(completed | {name} for name in ready)
        self.assertEqual(maximum, graph["expected_max_requirement_ready_frontier"])
        self.assertIn(frozenset(slots), seen)


if __name__ == "__main__":
    unittest.main()
