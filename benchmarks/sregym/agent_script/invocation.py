"""Parse the Codex invocation shared by deterministic test CLIs."""


class Invocation:
    def __init__(self, argv: list[str]) -> None:
        if not argv or argv[0] != "exec":
            raise ValueError(f"unsupported codex invocation: {argv[:3]}")
        rest = argv[1:]
        self.resume: str | None = None
        if rest[:1] == ["resume"]:
            if len(rest) < 2:
                raise ValueError("codex exec resume needs a thread id")
            self.resume = rest[1]
            rest = rest[3:] if rest[2:3] == ["-"] else rest[2:]
        self.model: str | None = None
        positional: list[str] = []
        takes_value = {"--model", "-m", "--config", "-c", "--output-schema", "-C", "--cd", "--sandbox", "-s"}
        index = 0
        while index < len(rest):
            token = rest[index]
            if token in takes_value:
                if token in {"--model", "-m"} and index + 1 < len(rest):
                    self.model = rest[index + 1]
                index += 2
                continue
            if not token.startswith("-"):
                positional.append(token)
            index += 1
        self.prompt_argument = positional[-1] if positional and positional[-1] != "-" else None
