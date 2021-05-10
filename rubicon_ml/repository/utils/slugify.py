import string


def slugify(value):
    # Remove punctuation
    value = value.translate(str.maketrans("", "", string.punctuation))
    # Remove any excess whitespace within the string
    value = " ".join(value.split()).strip()
    # Lowercase and replace spaces with our separator
    return value.lower().replace(" ", "-")
