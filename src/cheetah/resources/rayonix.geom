adu_per_eV = 0.1
clen = 0.1000

; Uncomment these lines to read data from file
; photon_energy = /LCLS/photon_energy_eV
; peak_list = /entry_1/result_1
; peak_list_type = cxi
; data = /entry_1/data_1/data
; dim0 = %
; dim1 = ss
; dim2 = fs

; Uncomment these lines when streaming data from OM to CrystFEL
photon_energy = beam_energy
peak_list = peak_list
data = detector_data
dim0 = ss
dim1 = fs

; Uncomment these lines if you have a separate bad pixel map (recommended!)
; mask_file = /path/to/mask.h5
; mask = /data/data
; mask_good = 0x1
; mask_bad = 0x0

p0a0/min_fs = 0
p0a0/max_fs = 1919
p0a0/min_ss = 0
p0a0/max_ss = 1919
p0a0/corner_x = 958.056501
p0a0/corner_y = -964.850036
p0a0/fs = 0.000000x +1.000000y +0.000000z
p0a0/ss = -1.000000x +0.000000y +0.000000z
p0a0/coffset = 0.000000
p0a0/res = 5649.717500