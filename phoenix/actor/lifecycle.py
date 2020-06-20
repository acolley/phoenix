import attr


class RestartActor(Exception):
    def __init__(self, behaviour):
        self.behaviour = behaviour


class StopActor(Exception):
    pass


@attr.s
class PreRestart:
    pass


@attr.s
class PostStop:
    pass
