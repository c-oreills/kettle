from wtforms import Form

class RolloutForm(Form):
    basic_field_names = ()
    advanced_field_names = ()

    @property
    def basic_fields(self):
        return [getattr(self, field_name) for field_name in self.basic_field_names]

    @property
    def advanced_fields(self):
        return [getattr(self, field_name) for field_name in self.advanced_field_names]
