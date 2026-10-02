"""New twelve-arm synthetic evidence checks; no runner or candidate imports."""
from collections import Counter
from copy import deepcopy
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from analysis.continuation_followup_comparison import (
    ANALYSIS_PATH, ANALYSIS_VERSION, AUDIT_PROTOCOL, CONTRASTS, Inputs, POLICY_CONTRACT,
    POLICIES, PROJECTS, PROTOCOL, allocation, digest, paired, requirement_summary, summarize,
)


class FollowupComparisonTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.make_cohort()

    def write(self, name, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def read(self, name):
        return json.loads((self.root / name).read_text())

    def sha(self, name):
        return hashlib.sha256((self.root / name).read_bytes()).hexdigest()

    def binding(self, name):
        return dict(path=str(self.root / name), sha256=self.sha(name))

    def change(self, name, edit):
        value = self.read(name)
        edit(value)
        self.write(name, value)

    def refresh(self):
        self.change("independent-audit.json", lambda row: row.update(results_sha256=self.sha("results.json"), timings_sha256=self.sha("timings.json")))

    def make_cohort(self, rehearsal=False):
        source = self.root / "source-snapshot" / ANALYSIS_PATH
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes((Path(__file__).parents[1] / ANALYSIS_PATH).read_bytes())
        frozen = self.root / "source-snapshot/gossip_harness/inert.py"
        frozen.parent.mkdir(parents=True, exist_ok=True)
        frozen.write_text("raise RuntimeError('Must never execute')\n")
        auditor=self.root/"source-snapshot/analysis/audit_continuation_followup.py"
        auditor.write_text("raise RuntimeError('Auditor must never execute')\n")
        taxonomy = {"warehouse": dict(baseline_inherited="W0-", milestone_1_added="W1-", milestone_2_added="W2-"),
                    "job-queue": dict(baseline_inherited="Q0-", milestone_1_added="Q1-", milestone_2_added="Q2-")}
        analysis = dict(version=ANALYSIS_VERSION, primary_contrasts=[list(pair) for pair in CONTRASTS.values()],
                        requirement_taxonomy=taxonomy, experimental_allocation_rule="Exact halves for shared independent builders; thirds for scouts; sequential builders assigned to sequential arm. Joint study only.")
        roster = [[project, policy, rep] for project in PROJECTS for rep in range(2) for policy in POLICIES]
        blocks = [[project, rep] for project in PROJECTS for rep in range(2)]
        plan = dict(protocol=PROTOCOL, projects=list(PROJECTS), policies=POLICY_CONTRACT, roster=roster, block_order=blocks, pilot_analysis=analysis)
        self.write("study-plan.json", plan)
        projects = []
        for project, names in zip(PROJECTS, (("W0-stock", "W0-stock", "W1-fefo", "W2-inherited"), ("Q0-state", "Q0-validation", "Q1-leases", "Q2-fanout"))):
            cases = [dict(id=f"{project}-{i}", requirement=name) for i, name in enumerate(names)]
            projects.append(dict(id=project, stages=[dict(hidden_cases=cases[:3]), dict(hidden_cases=cases[3:])]))
        self.write("fixtures.json", dict(projects=projects))
        contract = dict(protocol=PROTOCOL, policies=POLICY_CONTRACT, milestones=2, repetitions=2, roster=roster, block_order=blocks,
            pilot_analysis=analysis, plan_sha256=self.sha("study-plan.json"),
            sources={"inert.py": self.sha("source-snapshot/gossip_harness/inert.py")},
            extra_sources={ANALYSIS_PATH: self.sha("source-snapshot/" + ANALYSIS_PATH),
                           "analysis/audit_continuation_followup.py":self.sha("source-snapshot/analysis/audit_continuation_followup.py")},
            fixtures={project["id"]: dict(fixture_sha256=digest(project)) for project in projects})
        self.write("preregistered.json", dict(contract=contract))
        spans = []
        def span(kind, start, end, **labels):
            row = dict(kind=kind, span_id=len(spans), clock_id="one-clock", status="finished", started_monotonic_ns=int(start * 1e9),
                       finished_monotonic_ns=int(end * 1e9), elapsed_seconds=end-start, **labels)
            spans.append(row)
            return row
        def call(owner, role, start, usage, call_id):
            physical = span("scripted_worker" if rehearsal else "physical_provider_request", start, start+1, run_id=owner,
                            role=role, model="cheap", call_id=call_id)
            return dict(role=role, model="cheap", call_id=call_id, physical_dispatch=not rehearsal,
                        physical_span_id=None if rehearsal else physical["span_id"], usage_units=0 if rehearsal else usage,
                        session_identity=owner)
        shared, frozen_shared, audit_shared, manifest_index = [], [], [], {}
        for ordinal, (project, rep) in enumerate(blocks):
            start = ordinal * 30
            parent = span("shared_setup_block", start, start+20, run_id=f"{project}-{rep}-shared")
            manifest = dict(protocol="continuation-followup-shared-setup-v1", project_id=project, repetition=rep,
                            contract_sha256=digest(contract), pools={}, scouts={})
            calls = []
            for formation, offsets, costs in (("independent", [1,2,3,4], [2,1,1,1]), ("sequential", [6,7,8,9], [4,1,1,1]), ("scouts", [11,11.5], [2,3])):
                owner = f"{project}-{rep}-shared-{formation}"
                current = [call(owner, "scout" if formation == "scouts" else "builder", start+offset, cost, f"call-{i}")
                           for i,(offset,cost) in enumerate(zip(offsets,costs))]
                span("docker_validation", start+14, start+15, run_id=owner, purpose="stage_evidence")
                packet = dict(session_id=owner, invocations=current, usage_micro_usd=sum(c["usage_units"] for c in current),
                              physical_provider_calls=sum(c["physical_dispatch"] for c in current))
                prefix=f"shared/{project}-{rep}/{formation}"
                self.write(prefix+"/session.json", packet)
                self.write(prefix+"/initial-pool.json", {"formation":formation})
                binding = dict(session=self.binding(prefix+"/session.json"), initial_pool=self.binding(prefix+"/initial-pool.json"))
                if formation == "scouts":
                    manifest["scouts"] = binding
                else:
                    manifest["pools"][formation] = binding
                calls.extend(current)
            usage = sum(c["usage_units"] for c in calls)
            physical = sum(c["physical_dispatch"] for c in calls)
            manifest.update(usage_micro_usd=usage, physical_provider_calls=physical)
            name=f"shared/{project}-{rep}/shared-setup.json"
            self.write(name,manifest)
            binding=self.binding(name)
            manifest_index[(project,rep)]=(manifest,binding)
            shared.append(dict(project_id=project,repetition=rep,manifest=binding,invocations=calls,usage_micro_usd=usage,
                               physical_provider_calls=physical,span_id=parent["span_id"],elapsed_seconds=20))
            frozen_shared.append(dict(project_id=project,repetition=rep,manifest=binding))
            audit_shared.append(dict(project_id=project,repetition=rep,manifest_sha256=binding["sha256"],usage_micro_usd=usage,physical_provider_calls=physical))
        cases, trajectories, frozen_rows, audit_rows = [], [], [], []
        for ordinal,(project,policy,rep) in enumerate(roster):
            run_id=f"{project}-{policy}-{rep}"
            manifest,binding=manifest_index[(project,rep)]
            attribution=dict(manifest=binding,physical_provider_calls=0,charged_micro_usd=0,attributed_builder_opportunities=4,
                attributed_scout_opportunities=2,formation=POLICY_CONTRACT[policy]["formation"],
                initial_pool=manifest["pools"][POLICY_CONTRACT[policy]["formation"]]["initial_pool"])
            bounded = policy == "current-independent" and rep == 1 and not rehearsal
            stage_count=1 if bounded else 2
            start=200+ordinal*20
            parent=span("model_trajectory",start,start+10,run_id=run_id)
            stages,calls=[],[]
            for stage in range(stage_count):
                current=[call(run_id,"reviewer",start+stage*4+1,3,f"review-{stage}"), call(run_id,"builder",start+stage*4+2,2,f"repair-{stage}")]
                calls.extend(current)
                span("docker_validation",start+stage*4+3,start+stage*4+4,run_id=run_id,purpose="stage_evidence")
                improved=policy != "current-independent"
                comparison=dict(regression=stage==0,failure_progress=stage==1,resolved_failures=["a","b"] if stage==0 else ["c"],
                    introduced_failures=["c"] if stage==0 else [],predecessor="old",repaired="new")
                metrics=dict(repairs=1,effective_source_changes=1,retained_repair_checkpoints=int(improved),repair_regressions=int(improved and stage==0),
                    repair_failure_progress=int(improved and stage==1),resolved_known_failures=(2 if stage==0 else 1) if improved else 0,
                    introduced_failures=int(improved and stage==0))
                stages.append(dict(completed=not bounded,invocations=current,metrics=metrics,repair_comparisons=[comparison] if improved else [],
                    reason_code="accepted" if not bounded else "uncertainty_without_focused_evidence",remaining=[] if not bounded else ["new"],
                    shared_import=attribution if stage==0 else None))
            state=dict(stages=stages,shared_import=attribution)
            self.write(run_id+"/trajectory.json",state)
            trajectory=self.binding(run_id+"/trajectory.json")
            trajectories.append(dict(run_id=run_id,trajectory_sha256=trajectory["sha256"],span_id=parent["span_id"],elapsed_seconds=10,
                                     terminal="bounded_incomplete" if bounded else "visible_complete"))
            frozen_rows.append(dict(run_id=run_id,project_id=project,policy=policy,repetition=rep,trajectory=trajectory))
            accepted = rehearsal or policy=="improved-independent" or (policy=="improved-sequential" and rep==0)
            hidden=[case for p in projects if p["id"]==project for stage in p["stages"] for case in stage["hidden_cases"]]
            outcomes=[dict(**item,passed=accepted or i<len(hidden)-1) for i,item in enumerate(hidden)]
            private=dict(outcomes=outcomes,passed=all(row["passed"] for row in outcomes))
            visible=dict(passed=True)
            coverage={r:all(row["passed"] for row in outcomes if row["requirement"]==r) for r in {row["requirement"] for row in outcomes}}
            self.write(run_id+"/final-private-receipt.json",private)
            self.write(run_id+"/final-visible-receipt.json",visible)
            final_start=510+ordinal*4
            span("final_evaluation",final_start,final_start+3,run_id=run_id)
            span("docker_validation",final_start,final_start+1,run_id=run_id,purpose="final_visible")
            span("docker_validation",final_start+1,final_start+2,run_id=run_id,purpose="final_private")
            metrics={key:sum(stage["metrics"].get(key,0) for stage in stages) for key in stages[0]["metrics"]}
            cases.append(dict(run_id=run_id,project_id=project,policy=policy,repetition=rep,accepted=accepted,
                status="accepted" if accepted else "bounded_incomplete" if bounded else "final_quality_failed",milestones_completed=sum(stage["completed"] for stage in stages),
                trajectory_sha256=trajectory["sha256"],final_hidden=private,final_visible=visible,requirement_coverage=coverage,
                invocations=calls,usage_micro_usd=sum(c["usage_units"] for c in calls),physical_provider_calls=sum(c["physical_dispatch"] for c in calls),
                shared_import=attribution,metrics=metrics))
            audit_rows.append(dict(run_id=run_id,accepted=accepted,milestones_completed=sum(stage["completed"] for stage in stages),
                                   stages=[dict(completed=stage["completed"]) for stage in stages]))
        freeze=dict(protocol=PROTOCOL,contract_sha=digest(contract),trajectories=frozen_rows,shared_setups=frozen_shared,
                    private_evaluation_started=False,frozen_monotonic_ns=500_000_000_000)
        self.write("frozen-trajectories.json",freeze)
        self.write("timings.json",dict(clock_id="one-clock",spans=spans))
        for case in cases:
            self.write(case["run_id"]+"/result.json",case)
        cost=sum(row["usage_micro_usd"] for row in cases+shared)
        self.write("results.json",dict(experiment=PROTOCOL,status="finished",phase="finished",mode="rehearsal" if rehearsal else "live",
            contract=contract,contract_sha=digest(contract),freeze_manifest_sha256=self.sha("frozen-trajectories.json"),
            cases=cases,trajectories=trajectories,shared_setups=shared,unexecuted=[],censored=[],active_case=None,incremental_micro_usd=cost))
        self.write("independent-audit.json",dict(protocol=AUDIT_PROTOCOL,passed=True,mode="rehearsal" if rehearsal else "live",scope=dict(primary_cohort=True,live_model_quality=not rehearsal),
            results_sha256=self.sha("results.json"),timings_sha256=self.sha("timings.json"),freeze_manifest_sha256=self.sha("frozen-trajectories.json"),
            contract_sha256=digest(contract),auditor_sha256=contract["extra_sources"]["analysis/audit_continuation_followup.py"],cases=audit_rows,shared_setups=audit_shared,accounting=dict(study_usage_micro_usd=cost)))

    def test_twelve_arms_two_contrasts_four_blocks_not_independent_domains(self):
        result=summarize(self.root)
        self.assertEqual(result["application_domains"],2)
        self.assertEqual(len(result["trajectories"]),12)
        self.assertEqual(result["policies"]["improved-independent"]["accepted"],4)
        for contrast in result["contrasts"].values():
            self.assertEqual(contrast["quality_ordered"]["blocks"],4)
            self.assertEqual(len(contrast["quality_ordered"]["matches"]),4)
        self.assertIsNone(result["elo"])
        self.assertIsNone(result["population_p_values"])

    def test_inherited_groups_and_new_integration_are_separate(self):
        row=summarize(self.root)["trajectories"][0]
        categories=row["requirement_categories"]
        self.assertEqual(categories["baseline_inherited"]["total"],1)
        self.assertEqual(categories["all_added"]["total"],2)
        self.assertEqual(categories["milestone_2_added"]["groups"],{"W2-inherited":False})
        taxonomy=self.read("study-plan.json")["pilot_analysis"]["requirement_taxonomy"]
        with self.assertRaisesRegex(ValueError,"Unclassified"):
            requirement_summary({"unexpected":True},"warehouse",taxonomy)

    def test_shared_physical_work_counted_once_and_allocated_exactly(self):
        result=summarize(self.root)
        self.assertEqual(result["accounting"]["shared_setup_usage_micro_usd"],68)
        self.assertEqual(result["accounting"]["shared_physical_provider_calls"],40)
        self.assertEqual(result["accounting"]["total_physical_usage_micro_usd"],178)
        allocations=result["shared_setups"][0]["optional_joint_experiment_allocation"]
        values=[Fraction(row["usage_micro_usd"]["numerator"],row["usage_micro_usd"]["denominator"]) for row in allocations.values()]
        self.assertEqual(values,[Fraction(25,6),Fraction(25,6),Fraction(26,3)])
        self.assertEqual(sum(values),17)
        self.assertEqual(result["trajectories"][0]["imported_builder_opportunities"],4)
        self.assertFalse(result["accounting"]["allocation_is_policy_cost_ranking"])

    def test_timing_uses_cohort_spans_once_separates_wait_and_setup(self):
        # A misleading cumulative copy is irrelevant: authoritative timings.json owns measurement.
        self.write("shared/warehouse-0/timings.json",self.read("timings.json"))
        result=summarize(self.root)
        timing=result["trajectories"][0]["timing"]
        self.assertEqual((timing["arm_model_work_seconds"],timing["final_adjudication_seconds"],timing["arm_active_seconds"]),(10,3,13))
        self.assertEqual(timing["wait_after_shared_setup_seconds"],180)
        self.assertEqual(timing["wait_to_global_freeze_seconds"],290)
        self.assertEqual(result["measured_time"]["shared_setup"]["cumulative_seconds"],80)
        self.assertEqual(result["measured_time"]["physical_provider"]["overlap_seconds"],2)

    def test_bounded_stops_keep_denominator_and_no_success_time_is_null(self):
        policy=summarize(self.root)["policies"]["current-independent"]
        self.assertEqual((policy["trajectories"],policy["bounded_stops"],policy["accepted"]),(4,2,0))
        self.assertIsNone(policy["timing"]["accepted_arm_active"]["median_seconds"])

    def test_regression_remains_distinct_from_net_gain_and_current_unknown(self):
        policies=summarize(self.root)["policies"]
        self.assertEqual(policies["improved-independent"]["measured_same_case_regressions"],4)
        self.assertEqual(policies["improved-independent"]["measured_same_case_failure_progress"],4)
        self.assertIsNone(policies["current-independent"]["measured_same_case_regressions"])

    def test_scripted_rehearsal_is_zero_api_and_not_model_quality(self):
        self.make_cohort(rehearsal=True)
        result=summarize(self.root)
        self.assertEqual(result["evidence_class"],"scripted_infrastructure_control")
        self.assertFalse(result["model_quality_evidence"])
        self.assertEqual(result["accounting"]["total_physical_provider_calls"],0)
        self.assertEqual(result["accounting"]["total_physical_usage_micro_usd"],0)
        self.assertEqual(result["shared_setups"][0]["invocations"],10)

    def test_partial_cohort_does_not_read_final_receipts_or_score(self):
        report=self.read("results.json")
        self.change("results.json",lambda row:row.update(status="interrupted",censored=[dict(run_id=report["cases"][-1]["run_id"],reason="BudgetExceeded")]))
        for path in self.root.glob("*/final-private-receipt.json"):
            path.unlink()
        result=summarize(self.root)
        for key in ("policies","projects","contrasts","accounting"):
            self.assertIsNone(result[key])
        self.assertEqual(result["status"],"incomplete_unscored")
        self.assertEqual(Counter(r["execution_status"] for r in result["trajectories"])["censored"],1)

    def test_whole_cohort_certificate_required_not_setup_certificate(self):
        self.change("independent-audit.json",lambda row:row.update(protocol="independent-continuation-followup-setup-audit-v1",scope=dict(primary_cohort=False)))
        with self.assertRaisesRegex(ValueError,"whole-cohort audit"):
            summarize(self.root)

    def test_timing_certificate_exact_bytes_and_source_inventory(self):
        path=self.root/"timings.json"
        path.write_text(path.read_text()+"\n")
        with self.assertRaisesRegex(ValueError,"whole-cohort audit"):
            summarize(self.root)
        self.refresh()
        (self.root/"source-snapshot"/ANALYSIS_PATH).write_text("changed")
        with self.assertRaisesRegex(ValueError,"extra source changed"):
            summarize(self.root)

    def test_duplicate_arm_or_boolean_identity_cannot_fill_roster(self):
        for replacement in (deepcopy(self.read("results.json")["cases"][0]),None):
            original=self.read("results.json")
            altered=deepcopy(original)
            if replacement is None:
                altered["cases"][0]["repetition"]=False
            else:
                altered["cases"][1]=replacement
            self.write("results.json",altered)
            self.refresh()
            with self.assertRaisesRegex(ValueError,"twelve-arm roster"):
                summarize(self.root)
            self.write("results.json",original)

    def test_exact_pairing_and_quality_order_separate_coverage(self):
        rows=summarize(self.root)["trajectories"]
        for row in rows:
            row.update(accepted=row["policy"]=="improved-independent",requirements_passed=0 if row["policy"]=="improved-independent" else row["requirements_total"])
        left,right=CONTRASTS["controller_package"]
        self.assertEqual(paired(rows,left,right,"quality_ordered")["wins"],4)
        self.assertEqual(paired(rows,left,right,"requirement_coverage")["losses"],4)
        self.assertEqual(paired(rows,left,right,"quality_ordered"),paired(list(reversed(rows)),left,right,"quality_ordered"))

    def test_same_call_id_across_setup_sessions_is_not_deduplicated(self):
        result=summarize(self.root)
        self.assertEqual(result["shared_setups"][0]["physical_provider_calls"],10)
        self.assertEqual(result["shared_setups"][0]["parts"]["independent"]["physical_provider_calls"],4)
        self.assertEqual(result["shared_setups"][0]["parts"]["sequential"]["physical_provider_calls"],4)

    def test_import_cannot_be_charged_or_swapped(self):
        report=self.read("results.json")
        case=report["cases"][0]
        case["shared_import"]["charged_micro_usd"]=1
        self.write(case["run_id"]+"/result.json",case)
        self.write("results.json",report)
        self.refresh()
        with self.assertRaisesRegex(ValueError,"Imported setup"):
            summarize(self.root)

    def test_accounting_double_count_rejected(self):
        self.change("results.json",lambda row:row.update(incremental_micro_usd=row["incremental_micro_usd"]+68))
        self.refresh()
        with self.assertRaisesRegex(ValueError,"accounting does not reconcile"):
            summarize(self.root)

    def test_repeated_read_mutation_and_final_readset_rejected(self):
        inputs=Inputs(self.root)
        inputs.read("results.json")
        self.change("results.json",lambda row:row.update(extra=True))
        with self.assertRaisesRegex(ValueError,"between reads"):
            inputs.read("results.json")
        self.refresh()
        with patch.object(Inputs,"unchanged",return_value=False),self.assertRaisesRegex(ValueError,"Evidence changed"):
            summarize(self.root)

    def retain_known_failure(self, *, halted=False):
        report=self.read("results.json")
        case=report["cases"][1]
        run_id=case["run_id"]
        state=self.read(run_id+"/trajectory.json")
        timing=self.read("timings.json")
        call=case["invocations"][1]
        physical=next(row for row in timing["spans"] if row.get("run_id")==run_id
                      and row.get("call_id")==call["call_id"] and row["kind"] in ("scripted_worker","physical_provider_request"))
        physical["status"]="failed"
        wrapper={**physical,"kind":"worker_request","span_id":len(timing["spans"])}
        timing["spans"].append(wrapper)
        call.update(outcome="failure",metadata={"halt":halted},physical_span_id=physical["span_id"],request_span_id=wrapper["span_id"])
        state["stages"][0]["invocations"][1]=deepcopy(call)
        self.write(run_id+"/trajectory.json",state)
        case["trajectory_sha256"]=self.sha(run_id+"/trajectory.json")
        self.write(run_id+"/result.json",case)
        freeze=self.read("frozen-trajectories.json")
        next(row for row in freeze["trajectories"] if row["run_id"]==run_id)["trajectory"]=self.binding(run_id+"/trajectory.json")
        self.write("frozen-trajectories.json",freeze)
        report["freeze_manifest_sha256"]=self.sha("frozen-trajectories.json")
        next(row for row in report["trajectories"] if row["run_id"]==run_id)["trajectory_sha256"]=case["trajectory_sha256"]
        self.write("results.json",report)
        self.write("timings.json",timing)
        self.change("independent-audit.json",lambda row:row.update(freeze_manifest_sha256=self.sha("frozen-trajectories.json")))
        self.refresh()

    def test_handled_known_failure_remains_measured_in_completed_cohort(self):
        self.retain_known_failure()
        result=summarize(self.root)
        self.assertEqual(result["status"],"certified_complete")
        self.assertTrue(result["trajectories"][1]["accepted"])
        self.assertEqual(result["accounting"]["total_physical_usage_micro_usd"],178)

    def test_required_scripted_noop_failure_can_complete_rehearsal(self):
        self.make_cohort(rehearsal=True)
        self.retain_known_failure()
        result=summarize(self.root)
        self.assertEqual(result["evidence_class"],"scripted_infrastructure_control")
        self.assertEqual(result["accounting"]["total_physical_provider_calls"],0)

    def test_halted_or_unbound_failed_span_is_not_completed_work(self):
        self.retain_known_failure(halted=True)
        with self.assertRaisesRegex(ValueError,"settled known outcome"):
            summarize(self.root)

    def test_rehearsal_certificate_never_authorizes_live_quality_claim(self):
        self.change("independent-audit.json",lambda row:row.update(mode="rehearsal"))
        with self.assertRaisesRegex(ValueError,"whole-cohort audit"):
            summarize(self.root)
        self.change("independent-audit.json",lambda row:row.update(mode="live",scope=dict(primary_cohort=True,live_model_quality=False)))
        with self.assertRaisesRegex(ValueError,"whole-cohort audit"):
            summarize(self.root)

    def test_auditor_version_is_bound_to_frozen_contract(self):
        self.change("independent-audit.json",lambda row:row.update(auditor_sha256="different-version"))
        with self.assertRaisesRegex(ValueError,"whole-cohort audit"):
            summarize(self.root)

    def test_failed_unbound_stage_span_cannot_be_known_call_failure(self):
        self.change("timings.json",lambda row:row["spans"][0].update(status="failed"))
        self.refresh()
        with self.assertRaisesRegex(ValueError,"Shared setup parent"):
            summarize(self.root)


if __name__ == "__main__":
    unittest.main()
