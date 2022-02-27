from cobs import cobs


def bytes_to_int(inp):
    return int.from_bytes(inp, byteorder="little", signed=False)


class CobsHandler:
    zero_byte = bytes([0])

    def __init__(self, *args):
        self.elements = [x for x in args]

    def get_components(self, inp):

        num_zeros = inp.count(CobsHandler.zero_byte)
        if num_zeros != (len(self.elements) + 1):
            raise ValueError(f"why god {inp} {self.elements}")

        splitted = inp.split(CobsHandler.zero_byte)

        if len(splitted) != (2 + len(self.elements)):
            raise ValueError("Wrong number of elements")

        if len(splitted[0]) != 0:
            raise ValueError("Head is wrong")

        if len(splitted[-1]) != 0:
            raise ValueError("Tail is wrong")

        component_dict = dict()
        for entry_name, entry in zip(self.elements, splitted[1:-1]):
            component_dict[entry_name] = cobs.decode(entry)
        return component_dict

    @staticmethod
    def _encode_element(element, force=False):

        if isinstance(element, bytes) or isinstance(element, bytearray):
            return element
        elif isinstance(element, str):
            return element.encode()
        elif isinstance(element, int):
            bits = element.bit_length()
            num_bytes = bits // 8 + 1 if bits % 8 != 0 else 0
            return element.to_bytes(num_bytes, byteorder="little", signed=False)
        if force:
            return bytes(element)
        return None

    @staticmethod
    def encode(inp):
        m = CobsHandler._encode_element(inp)
        if m is None:
            m = CobsHandler.zero_byte.join(
                map(lambda x: cobs.encode(CobsHandler._encode_element(x, True)), inp)
            )
        return CobsHandler.zero_byte + m + CobsHandler.zero_byte
