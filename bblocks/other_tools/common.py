from dataclasses import dataclass, field

from country_converter import convert


@dataclass(repr=False)
class Dict(dict):
    dictionary: dict = field(default_factory=dict)

    """A wrapper that adds functionality to a standard dictionary"""

    def __post_init__(self):
        self.update(self.dictionary)

    def change_keys(self, from_: str = None, to: str = "ISO3") -> dict:
        _ = {
            convert(names=key, src=from_, to=to, not_found=None): value
            for key, value in self.dictionary.items()
        }
        self.clear()
        self.update(_)
        return self

    def reverse(self) -> dict:
        _ = {value: key for key, value in self.items()}
        self.clear()
        self.update(_)
        return self

    def set_keys_type(self, type_: type) -> dict:
        _ = {type_(key): value for key, value in self.dictionary.items()}
        self.clear()
        self.update(_)
        return self

    def set_values_type(self, type_: type) -> dict:
        _ = {key: type_(value) for key, value in self.dictionary.items()}
        self.clear()
        self.update(_)
        return self
