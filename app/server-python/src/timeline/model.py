TICKS_PER_SECOND = 5
WINDOW_RADIUS = 4  # numbers shown on each side of the current value


def _linear(i: int) -> int:
    """0, 1, 2, 3, 4, ..."""
    return i


def _fibonacci(i: int) -> int:
    """0, 1, 1, 2, 3, 5, 8, ..."""
    a, b = 0, 1
    for _ in range(i):
        a, b = b, a + b
    return a


def _is_prime(n: int) -> bool:
    if n < 2:
        return False
    if n < 4:
        return True
    if n % 2 == 0:
        return False
    factor = 3
    while factor * factor <= n:
        if n % factor == 0:
            return False
        factor += 2
    return True


def _prime(i: int) -> int:
    """0-indexed nth prime: 2, 3, 5, 7, 11, ..."""
    count = -1
    n = 1
    while count < i:
        n += 1
        if _is_prime(n):
            count += 1
    return n


# Display name -> function mapping index position to the value shown.
SEQUENCES = {
    "Linear": _linear,
    "Primes": _prime,
    "Fibonacci": _fibonacci,
}

DEFAULT_SEQUENCE = "Linear"


class TimelineModel:
    def __init__(self, sequence: str = DEFAULT_SEQUENCE, start: int = 0):
        # Each sequence remembers its own position for the lifetime of the run.
        self._indices = {name: max(0, start) for name in SEQUENCES}
        self.set_sequence(sequence)

    def set_sequence(self, name: str) -> None:
        self.sequence_name = name
        self._fn = SEQUENCES[name]

    @property
    def index(self) -> int:
        return self._indices[self.sequence_name]

    @index.setter
    def index(self, value: int) -> None:
        self._indices[self.sequence_name] = max(0, value)

    def step_forward(self) -> None:
        self.index += 1

    def step_back(self) -> None:
        self.index -= 1

    def value_at(self, index: int) -> int | None:
        """Value at a position, or None for positions before the start."""
        if index < 0:
            return None
        return self._fn(index)

    def visible_window(self) -> list[int | None]:
        """Return the values visible in the timeline window.

        Each entry is an int, or None if its position is negative.
        """
        return [
            self.value_at(i)
            for i in range(self.index - WINDOW_RADIUS, self.index + WINDOW_RADIUS + 1)
        ]
