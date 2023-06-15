from gtile.sat import asf

def rcs_and_dem(aoi, toi=None, uuid=None, products=None): 
    
    if products is None: 
        # Search for products:
        assert len(toi) == 2
        t0, t1 = toi
        qurey = {
            "processingLevel": "GRD_HD",
            "intersectsWith": aoi,
            "start": t0,
            "end": t1
            }
        products = asf.search(qurey)
    print(products)
    
    
    