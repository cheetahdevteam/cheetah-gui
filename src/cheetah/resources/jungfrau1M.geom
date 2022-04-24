adu_per_eV = 0.000066  ; 42 adu/keV for highest gain mode
clen =  0.100
photon_energy = 12000
;photon_energy_bandwidth = 0.1
;max_adu = 7500

res = 13333.33

;mask_file = /path/to/mask.h5
;mask = /data/data
;mask_good = 0x01
;mask_bad = 0x00

data = /data/data
dim0 = %
dim1 = ss
dim2 = fs

rigid_group_p1 = p1a1,p1a2,p1a3,p1a4,p1a5,p1a6,p1a7,p1a8
rigid_group_p2 = p2a1,p2a2,p2a3,p2a4,p2a5,p2a6,p2a7,p2a8
rigid_group_collection_det = p1,p2

p2a1/corner_x = -590.472758
p2a1/corner_y = 19.078763
p2a1/fs = +1.000000x -0.000841y
p2a1/ss = +0.000841x +1.000000y
p2a1/min_fs = 0
p2a1/max_fs = 255
p2a1/min_ss = 512
p2a1/max_ss = 767

p2a2/corner_x = -332.472758
p2a2/corner_y = 18.861863
p2a2/fs = +1.000000x -0.000841y
p2a2/ss = +0.000841x +1.000000y
p2a2/min_fs = 256
p2a2/max_fs = 511
p2a2/min_ss = 512
p2a2/max_ss = 767

p2a3/corner_x = -74.472758
p2a3/corner_y = 18.644963
p2a3/fs = +1.000000x -0.000841y
p2a3/ss = +0.000841x +1.000000y
p2a3/min_fs = 512
p2a3/max_fs = 767
p2a3/min_ss = 512
p2a3/max_ss = 767

p2a4/corner_x = 183.527242
p2a4/corner_y = 18.428063
p2a4/fs = +1.000000x -0.000841y
p2a4/ss = +0.000841x +1.000000y
p2a4/min_fs = 768
p2a4/max_fs = 1023
p2a4/min_ss = 512
p2a4/max_ss = 767

p2a5/corner_x = -590.255758
p2a5/corner_y = 277.078563
p2a5/fs = +1.000000x -0.000841y
p2a5/ss = +0.000841x +1.000000y
p2a5/min_fs = 0
p2a5/max_fs = 255
p2a5/min_ss = 768
p2a5/max_ss = 1023

p2a6/corner_x = -332.255758
p2a6/corner_y = 276.862563
p2a6/fs = +1.000000x -0.000841y
p2a6/ss = +0.000841x +1.000000y
p2a6/min_fs = 256
p2a6/max_fs = 511
p2a6/min_ss = 768
p2a6/max_ss = 1023

p2a7/corner_x = -74.255758
p2a7/corner_y = 276.645563
p2a7/fs = +1.000000x -0.000841y
p2a7/ss = +0.000841x +1.000000y
p2a7/min_fs = 512
p2a7/max_fs = 767
p2a7/min_ss = 768
p2a7/max_ss = 1023

p2a8/corner_x = 183.744242
p2a8/corner_y = 276.428563
p2a8/fs = +1.000000x -0.000841y
p2a8/ss = +0.000841x +1.000000y
p2a8/min_fs = 768
p2a8/max_fs = 1023
p2a8/min_ss = 768
p2a8/max_ss = 1023

p1a1/corner_x = -591.259758
p1a1/corner_y = -530.970437
p1a1/fs = +1.000000x -0.000641y
p1a1/ss = +0.000641x +1.000000y
p1a1/min_fs = 0
p1a1/max_fs = 255
p1a1/min_ss = 0
p1a1/max_ss = 255

p1a2/corner_x = -333.259758
p1a2/corner_y = -531.136437
p1a2/fs = +1.000000x -0.000641y
p1a2/ss = +0.000641x +1.000000y
p1a2/min_fs = 256
p1a2/max_fs = 511
p1a2/min_ss = 0
p1a2/max_ss = 255

p1a3/corner_x = -75.259758
p1a3/corner_y = -531.301437
p1a3/fs = +1.000000x -0.000641y
p1a3/ss = +0.000641x +1.000000y
p1a3/min_fs = 512
p1a3/max_fs = 767
p1a3/min_ss = 0
p1a3/max_ss = 255

p1a4/corner_x = 182.740242
p1a4/corner_y = -531.467437
p1a4/fs = +1.000000x -0.000641y
p1a4/ss = +0.000641x +1.000000y
p1a4/min_fs = 768
p1a4/max_fs = 1023
p1a4/min_ss = 0
p1a4/max_ss = 255

p1a5/corner_x = -591.093758
p1a5/corner_y = -272.970437
p1a5/fs = +1.000000x -0.000641y
p1a5/ss = +0.000641x +1.000000y
p1a5/min_fs = 0
p1a5/max_fs = 255
p1a5/min_ss = 256
p1a5/max_ss = 511

p1a6/corner_x = -333.093758
p1a6/corner_y = -273.136437
p1a6/fs = +1.000000x -0.000641y
p1a6/ss = +0.000641x +1.000000y
p1a6/min_fs = 256
p1a6/max_fs = 511
p1a6/min_ss = 256
p1a6/max_ss = 511

p1a7/corner_x = -75.093758
p1a7/corner_y = -273.301437
p1a7/fs = +1.000000x -0.000641y
p1a7/ss = +0.000641x +1.000000y
p1a7/min_fs = 512
p1a7/max_fs = 767
p1a7/min_ss = 256
p1a7/max_ss = 511

p1a8/corner_x = 182.906242
p1a8/corner_y = -273.467437
p1a8/fs = +1.000000x -0.000641y
p1a8/ss = +0.000641x +1.000000y
p1a8/min_fs = 768
p1a8/max_fs = 1023
p1a8/min_ss = 256
p1a8/max_ss = 511








