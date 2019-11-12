import re
import sys


for line in sys.stdin:
    line = line.strip()
    player = line.split(',')[-2]
    defenders = line.split(',')[15:17]
    defender = defenders[0]+' '+defenders[1]
    shot_hit = line.split(',')[14]
    # print("%s\t%s" % (player+'_'+defender+'_'+shot_hit, 1))
    print(player, '-', defender, '-', shot_hit)
