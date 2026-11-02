# Instruction budgets

Use the repository-owned [`sdo/agents/prompt_budgets.toml`](../../../../sdo/agents/prompt_budgets.toml)
for limits and reasons; do not duplicate its numbers here. If the file has not
landed during the template migration, report the missing budget contract.

Measure production-rendered Unicode code points for every named part, and
verify that joining the parts reproduces the exact final prompt. Measure the
actual skill content and typical/capped request data separately. Include
continuation and reflection input, not only the initial responder request.
A character count is not a model token or cost estimate.

Run the prompt golden-render and budget tests maintained beside the prompt
builders. A budget increase belongs in its own commit with an updated reason
and a `Budget-Reason:` trailer; inspect the diff before accepting an increase.
