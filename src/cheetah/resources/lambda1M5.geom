clen = 0.2
photon_energy = 16000
;max_adu = 100000

adu_per_photon = 1
res = 18181.8   ; 55 micron pixel size

; These lines describe the data layout for the Eiger native multi-event files
dim0 = %
dim1 = ss
dim2 = fs
data = /entry/data/data

; Uncomment these lines if you have a separate bad pixel map (recommended!)
;mask_file = /path/to/mask.h5
;mask = /data/data
;mask_good = 0x1
;mask_bad = 0x0


rigid_group_d0 = panel0,panel1
rigid_group_collection_det = d0

; corner_{x,y} set the position of the corner of the detector (in pixels)
; relative to the beam

panel0/min_fs = 0 
panel0/min_ss = 0 
panel0/max_fs = 1555
panel0/max_ss = 515
panel0/corner_x = -780
panel0/corner_y = -520
panel0/fs = +1.000000x +0.000000y 
panel0/ss = +0.000000x +1.000000y

panel1/min_fs = 0 
panel1/min_ss = 516 
panel1/max_fs = 1555
panel1/max_ss = 1031
panel1/corner_x = -780
panel1/corner_y = 150
panel1/fs = +1.000000x +0.000000y 
panel1/ss = +0.000000x +1.000000y

