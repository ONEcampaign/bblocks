from dataclasses import dataclass, field
from country_converter import convert


@dataclass
class Dict:
    dictionary: dict = field(default_factory=dict)
    """A wrapper that adds functionality to a standard dictionary"""

    def change_keys(self, from_: str = "regex", to: str = "ISO3") -> dict:
        self.dictionary = {
            convert(names=key, src=from_, to=to, not_found=None): value
            for key, value in self.dictionary.items()
        }
        return self.dictionary

    def reverse(self) -> dict:
        self.dictionary = {value: key for key, value in self.dictionary.items()}
        return self.dictionary

    def set_keys_type(self, type_: type) -> dict:
        self.dictionary = {type_(key): value for key, value in self.dictionary.items()}
        return self.dictionary

    def set_values_type(self, type_: type) -> dict:
        self.dictionary = {key: type_(value) for key, value in self.dictionary.items()}
        return self.dictionary
