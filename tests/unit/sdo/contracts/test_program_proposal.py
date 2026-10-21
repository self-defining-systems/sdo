from sdo.contracts.proto import (
    CorpusBinding,
    CorpusCaptureReference,
    OperationalArtifactProposal,
    ProposedCorpusCase,
    ProposedGoSource,
    ProposedLibrary,
    ProposedPlaybook,
    ProposedProgram,
    parse_json,
    to_canonical_json,
)


def test_program_and_library_proposal_round_trip() -> None:
    proposal = OperationalArtifactProposal(
        playbooks=[
            ProposedPlaybook(
                id="targetport",
                fault_class="targetport",
                program=ProposedProgram(
                    sources=[ProposedGoSource(name="diagnose.go", content="package targetport")],
                    corpus_cases=[ProposedCorpusCase(id="case-1", open_json="{}", labels_json="{}")],
                ),
            )
        ],
        libraries=[
            ProposedLibrary(
                package="ports",
                sources=[ProposedGoSource(name="ports.go", content="package ports")],
            )
        ],
    )

    decoded = parse_json(to_canonical_json(proposal), OperationalArtifactProposal())

    assert decoded == proposal


def test_capture_reference_round_trip() -> None:
    proposal = ProposedCorpusCase(
        id="captured",
        reference=CorpusCaptureReference(
            incident_id="incident",
            capture_label="open@pre-apply",
            open_sha256="a" * 64,
            fixed_sha256="b" * 64,
            recordings_sha256="c" * 64,
            root_cause_program_ids=["targetport"],
            bindings=[CorpusBinding(name="service", object={"kind": "Service", "namespace": "app", "name": "api"})],
        ),
    )
    assert parse_json(to_canonical_json(proposal), ProposedCorpusCase()) == proposal
