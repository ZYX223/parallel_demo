
def enum(*sequential, **named):
    """Handy way to fake an enumerated type in Python
    http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
    """
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)


# Define MPI message tags
tags = enum('READY', 'DONE', 'EXIT', 'START','EXEC','FLY','HIT',
            'CAR_EXEC','LAUNCH_REQUIRE','CAR_RES','LAUNCH_INFO',
            'UPDATE_MISSILE','MISSILE_POS','MISSILE_REPLACE')

if __name__ == "__main__":
    pass