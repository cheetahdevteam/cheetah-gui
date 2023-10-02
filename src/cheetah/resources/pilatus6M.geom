photon_energy = 12000
adu_per_eV = 0.0001
clen = 0.1
coffset = 0.0
res = 5814.0  ; 172 micron pixel size

rigid_group_d0 = 0
rigid_group_collection_quadrants = d0
rigid_group_collection_asics = d0

; For data processing from files:
;dim0 = %
;dim1 = ss
;dim2 = fs
;data = /data/data
;peak_list = /data/peaks
;peak_list_type = cxi

; For streaming from Cheetah to CrystFEL:
peak_list = peak_list
dim0 = ss
dim1 = fs
data = detector_data


0/min_fs = 0
0/max_fs = 2462
0/min_ss = 0
0/max_ss = 2526
0/corner_x = -1257
0/corner_y = -1240
0/fs = +1.000000x +0.000000y
0/ss = +0.000000x +1.000000y
