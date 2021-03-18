@dataclass
class Joining:
    name: str
    address: Tuple[str, int]


@dataclass
class Leading:
    name: str
    address: Tuple[str, int]


@dataclass
class Following:
    name: str
    address: Tuple[str, int]


Node = Union[Joining, Leading, Following, Leaving]


@dataclass
class Cluster:
    nodes: List[Node]
