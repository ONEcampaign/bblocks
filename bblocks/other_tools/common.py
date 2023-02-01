from country_converter import convert


class Dict(dict):

    """A wrapper that adds functionality to a standard dictionary"""

    def __repr__(self):
        # Return the elements with a line break between them
        return "{\n" + ",\n".join(f"{k}: {v}" for k, v in self.items()) + "\n}"

    def change_keys(self, from_: str = None, to: str = "ISO3") -> dict:
        _ = {
            convert(names=key, src=from_, to=to, not_found=None): value
            for key, value in self.items()
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
        _ = {type_(key): value for key, value in self.items()}
        self.clear()
        self.update(_)
        return self

    def set_values_type(self, type_: type) -> dict:
        _ = {key: type_(value) for key, value in self.items()}
        self.clear()
        self.update(_)
        return self
