from sdo.contracts.proto import (
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
