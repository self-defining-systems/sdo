Incident ${incident_id} in namespace ${namespace}.
${symptom}
Read your skills: ${skills_root}/incident-response/SKILL.md and ${skills_root}/sdo-operating/SKILL.md.
Start with `sdo detect`. Name every plausible explanation for what users see. Start one subagent per explanation (in the background) to test it read-only with evidence, and brief each to check whether the data and state on its component match what the failing operation needs, not only whether it is up. You are the orchestrator: do not investigate yourself, weigh the subagents' evidence, and make the fix only when the evidence points to one cause. Combine facts across reports, not only verdicts. Return only to give up.
${recent_changes}
${branch_instruction}
