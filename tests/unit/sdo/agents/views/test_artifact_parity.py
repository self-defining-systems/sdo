"""The reflection draft must carry exactly the protobuf artifact shape."""

from typing import get_args, get_origin

from google.protobuf.descriptor import FieldDescriptor

from sdo.agents.views import OperationalArtifactDraft
from sdo.contracts import OperationalArtifactProposal


def test_artifact_draft_matches_nested_proto_fields() -> None:
    def walk(view: type, descriptor: object) -> None:
        fields = {field.name: field for field in descriptor.fields}
        annotations = {
            (model_field.alias or name): model_field.annotation for name, model_field in view.model_fields.items()
        }
        assert set(annotations) == set(fields), descriptor.full_name
        for name, field in fields.items():
            annotation = annotations[name]
            if field.is_repeated:
                assert get_origin(annotation) is list
                annotation = get_args(annotation)[0]
            if field.message_type is not None:
                if type(None) in get_args(annotation):
                    assert field.has_presence
                    assert not field.is_repeated
                    annotation = next(item for item in get_args(annotation) if item is not type(None))
                walk(annotation, field.message_type)
            elif field.has_presence and not field.is_repeated:
                assert type(None) in get_args(annotation)
            if field.message_type is None and field.enum_type is None:
                scalar_type = {
                    FieldDescriptor.TYPE_STRING: str,
                    FieldDescriptor.TYPE_BOOL: bool,
                    FieldDescriptor.TYPE_INT32: int,
                    FieldDescriptor.TYPE_INT64: int,
                    FieldDescriptor.TYPE_UINT64: int,
                    FieldDescriptor.TYPE_DOUBLE: float,
                }[field.type]
                assert annotation is scalar_type or scalar_type in get_args(annotation)
            if field.enum_type is not None:
                assert {item.number for item in field.enum_type.values} == set(annotation)

    walk(OperationalArtifactDraft, OperationalArtifactProposal.DESCRIPTOR)
