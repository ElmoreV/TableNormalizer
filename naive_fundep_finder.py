# Try to find everything using a 2ˆN to 2ˆN relation

# so if you have 4 attributes, then you can make 2^4 = 16 different subsets 
# so this is the powerset, which is a set of all subsets of a set
# example: all attributes OMEGA = {A, B, C, D}
# and powerset would be {{}, {A}, ..., {D}, {A, B}, ..., {A, C, D}, {B, C, D},{A, B, C, D}}

# and all relations would be X->Y where X is an element of the powerset / a subset of the attributes
# and Y as well.
# example: AB->CD, A->A, ABCD -> A, ABCD->BCD, etc. etc.


# a nice representation of the powerset is a bitmap
# example:
# {A} = 0001
# {B} = 0010
# {C} = 0100
# {D} = 1000
# {A, B} = 0011
# {B, C, D} = 0111

# and a relation between two attribute sets can be represented as
# {A, B} -> {C, D}
# 0011 | 1100
# Or {A} -> {D}
# 0001 | 1000

# Every functional dependency can be either: shown to exist, shown to not exist, or unknown.

# There are a couple of rules, the Armstrong axioms, which are:
# 1. Reflexivity  if XY:(1)100 -> X:(1)000 and XY:1(1)00 -> Y:0(1)00
# 2. Augmentation if X:1100 -> Y:0010 then XZ:110(1) -> YZ:001(1) (here Z:0001)
# 3. Transitivity if X:1100 -> Y:1000 and Y:1000 -> Z:0100 then X:1100 -> Z:0100


def query_fd_map(hashmap,lhs_set, rhs_set):
    return hashmap[(convert_to_bitmap(lhs_set), convert_to_bitmap(rhs_set))]

def convert_to_bitmap(fd_set):
    bitmap = 0 
    for fd in fd_set:
        bitmap = bitmap | (1 << fd)
    return bitmap

def convert_from_bitmap(bitmap,N_attributes):
    fd_set = []
    for fd in range(N_attributes):
        if bitmap & (1 << fd):
            fd_set.append(fd)
    return fd_set

def exactly_one_bit_set(integer):
    """
    if integer is  = 0010000
    then integer-1 = 0001111
    so integer & integer-1 = 0000000    
    if integer is  = 0010010
    then integer-1 = 0010001
    then integer & integer-1 = 0010000 =/= 0
    Exception: if integer = 00000000, then it fails
    """
    if integer == 0:
        return False
    else:
        return (integer & integer -1) == 0    

def setup_hashmap(N_attributes):
    N_powerset = 2**N_attributes
    hashmap = {}
    print(f"Setting up hashmap with {N_powerset**2/1e3}k elements")
    for lhs in range(N_powerset):
        for rhs in range(N_powerset):
            # All start off unknown
            hashmap[(lhs,rhs)] = 0 
            if lhs == rhs:
                hashmap[(lhs,rhs)] = 1 # all sets of attributes uniquely identify themselves
                continue
            # if r is a subet of l, then l->r is true, reflexifity
            if (lhs | rhs)==lhs:
                hashmap[(lhs,rhs)] = 1 # all subsets are unique identified by the superset
    return hashmap
    
def insert_known_pairwise_fds(hashmap, known_fds,N_attributes):
    for lhs in range(1,N_attributes):
        for rhs in range(1,N_attributes):
            # absense in the known_pairwise_bitmap means it's a non-fd:
            # print(f"{convert_from_bitmap(l)}-/->{convert_from_bitmap(r)}")
            hashmap[(1<<lhs,1<<rhs)] = -1
    print("Setup known fds")
    # insert known fds
    known_fds_bitmap = []
    for fd in known_fds:
        known_fds_bitmap.append([1<<fd[0],1<<fd[1]])

    for fd_bitmap in known_fds_bitmap:
    # print(f"{convert_from_bitmap(fd_bitmap[0])}-->{convert_from_bitmap(fd_bitmap[1])}")
        hashmap[(fd_bitmap[0],fd_bitmap[1])] = 1
    return hashmap


def apply_union(hashmap, N_attributes):
    print(f"Union")
    N_powerset = 2**N_attributes

    # Now we need to propagate alls the known fds to the unknown fds
    # first: do composition/union L->R1, L->R2, L->R1R2
    
    # When L: 0001 -> R1: 0010
    # And L: 0001 -> R2: 0101
    # Then L: 0001 -> R1|R2: 0111
    for l in range(1,N_powerset):
        for r1 in range(1,N_powerset):
            for r2 in range(1,N_powerset):
                if r1 <= r2:
                    continue
                if not hashmap[(l,r1|r2)] ==0:
                    continue
                # propagate known fds
                if hashmap[(l,r1)] == 1 and hashmap[(l,r2)] == 1:
                    # print(f"{convert_from_bitmap(l)}-->{convert_from_bitmap(r1|r2)} by {convert_from_bitmap(l)}-->{convert_from_bitmap(r1)} and {convert_from_bitmap(l)}-->{convert_from_bitmap(r2)}")
                    hashmap[(l,r1|r2)] = 1
                # propagate known non-fds
                if hashmap[(l,r1)] == -1 or  hashmap[(l,r2)] == -1:
                    # print(f"{convert_from_bitmap(l)}-/->{convert_from_bitmap(r1|r2)} by {convert_from_bitmap(l)}-/->{convert_from_bitmap(r1)} and {convert_from_bitmap(l)}-->{convert_from_bitmap(r2)}")
                    hashmap[(l,r1|r2)] = -1
    return hashmap

def apply_augmentation(hashmap, N_attributes):
    print(f"Augmentation")

    N_powerset = 2**N_attributes

    # # second: do augmentation
    for lhs in range(1,N_powerset):
        for rhs in range(1,N_powerset):
            for third in range(1,N_powerset):
                # if third == r or third == l:
                #     continue
                # for all sets Z: if A->B then  AZ-> BZ
                if hashmap[(lhs,rhs)] == 1:
                    # print(f"{convert_from_bitmap(lhs|third)}-->{convert_from_bitmap(rhs|third)} by {convert_from_bitmap(lhs)}-->{convert_from_bitmap(rhs)}")
                    hashmap[(lhs|third,rhs|third)] = 1
                # converse augmentation:
                # if any set Z: AZ-/-> BZ then A-/->B
                if hashmap[(lhs|third,rhs|third)] == -1:
                    # print(f"{convert_from_bitmap(lhs)}-/->{convert_from_bitmap(rhs)} by {convert_from_bitmap(lhs|third)}-/->{convert_from_bitmap(rhs|third)}")
                    hashmap[(lhs,rhs)] = -1
                    
    return hashmap

def apply_transitivity(hashmap, N_attributes):
    print(f"Transitivity")
    N_powerset = 2**N_attributes
    for lhs in range(1,N_powerset):
        for rhs in range(1,N_powerset):
            for middle in range(1,N_powerset):
                #  X->Y and Y->Z then X->Z
                if hashmap[(lhs,middle)] == 1 and hashmap[(middle,rhs)] == 1:
                    hashmap[(lhs,rhs)] = 1
                # converse transitivity: X-/->Z and Y-->Z then X-/->Y
                if hashmap[(lhs,rhs)] == -1:
                    if hashmap[(lhs,middle)] == 1:
                        hashmap[(middle,rhs)] = -1
                    if hashmap[(middle,rhs)] == 1:
                        hashmap[(lhs,middle)] = -1
    return hashmap