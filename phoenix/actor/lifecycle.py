class RestartActor(Exception):
    def __init__(self, behaviour):
        self.behaviour = behaviour


class StopActor(Exception):
    pass
