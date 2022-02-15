#include "includes.h"
#include "datatypes.h"

int GoodbyeMessage(struct info *, const char *, ... );


int ApplyMagicFilter(struct info *info, GByte  **realRaster, vsi_l_offset *pnOutDataLength, char *func, char *params ){
	GByte     	*blob     = NULL;
	size_t           blobSize = 0;
	request_rec      *r       = info->r;

	// Read
	MagickWandGenesis();
	MagickWand      	*mt = NewMagickWand();
	MagickReadImageBlob( mt, *realRaster, *pnOutDataLength);

	// Apply
	
	 
	if 	( ! strcmp(func, "MagickAutoGammaImage" 	 ) ) MagickAutoGammaImage(mt); 
	else if ( ! strcmp(func, "MagickAutoLevelImage" 	 ) ) MagickAutoLevelImage(mt);
	else if ( ! strcmp(func, "MagickBrightnessContrastImage" ) ) {
		double brightness = 0; double contrast = 0;
		if ( sscanf(params, "%lf,%lf", &brightness, &contrast) != 2 ){ 
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Function %s needs 2 paramters!", func); GoodbyeMessage(info, "Function %s needs 2 paramters!", func);	
			if(mt) DestroyMagickWand(mt); MagickWandTerminus(); return 400; 
		}
		MagickBrightnessContrastImage(mt, brightness, contrast); 
	}
	else if ( ! strcmp(func, "MagickEdgeImage" ) ) {
		double radius = 0; 
		if ( sscanf(params, "%lf", &radius ) != 1 ){ 
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Function %s needs 1 paramter!", func); GoodbyeMessage(info, "Function %s needs 1 paramter!", func);	
			if(mt) DestroyMagickWand(mt); MagickWandTerminus(); return 400; 
		}
		MagickEdgeImage(mt, radius);
	} 
	else if ( ! strcmp(func, "MagickAdaptiveBlurImage" ) ) {
		double radius = 0; double sigma = 0; 
		if ( sscanf(params, "%lf,%lf", &radius, &sigma ) != 2 ){ 
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Function %s needs 2 paramters!", func); GoodbyeMessage(info, "Function %s needs 2 paramters!", func);	
			if(mt) DestroyMagickWand(mt); MagickWandTerminus(); return 400; 
		}
		MagickAdaptiveBlurImage(mt, radius, sigma);
	} 
	else if ( ! strcmp(func, "MagickAdaptiveSharpenImage" ) ) {
		double radius = 0; double sigma = 0; 
		if ( sscanf(params, "%lf,%lf", &radius, &sigma ) != 2 ){ 
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Function %s needs 2 paramters!", func); GoodbyeMessage(info, "Function %s needs 2 paramters!", func);	
			if(mt) DestroyMagickWand(mt); MagickWandTerminus(); return 400; 
		}
		MagickAdaptiveSharpenImage(mt, radius, sigma);
	} 
	else if ( ! strcmp(func, "MagickAddNoiseImage" ) ) {
		NoiseType noise_type; double  attenuate = 0; char *noise = malloc(100); bzero(noise, 99);
		if ( sscanf(params, "%[^','],%lf", noise, &attenuate ) != 2 ){ 
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Function %s needs 2 paramters!", func); GoodbyeMessage(info, "Function %s needs 2 paramters!", func);	
			if(mt) DestroyMagickWand(mt); MagickWandTerminus(); return 400; 
		}

		if 	( ! strcmp(noise, "Uniform"  		)) noise_type = UniformNoise;
		else if ( ! strcmp(noise, "Gaussian" 		)) noise_type = GaussianNoise;
		else if ( ! strcmp(noise, "Multiplicative" 	)) noise_type = MultiplicativeGaussianNoise;	
		else if ( ! strcmp(noise, "Impulse" 		)) noise_type = ImpulseNoise;
		else if ( ! strcmp(noise, "Laplacian" 		)) noise_type = LaplacianNoise;
		else if ( ! strcmp(noise, "Poisson" 		)) noise_type = PoissonNoise;
		else						   noise_type = UndefinedNoise;
		MagickAdaptiveSharpenImage(mt, noise_type, attenuate);
	} 
	else if ( ! strcmp(func, "MagickOilPaintImage" ) ) {
		double threshold = 0; 
		if ( sscanf(params, "%lf", &threshold ) != 1 ){ 
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Function %s needs 1 paramters!", func); GoodbyeMessage(info, "Function %s needs 1 paramters!", func);	
			if(mt) DestroyMagickWand(mt); MagickWandTerminus(); return 400; 
		}
		MagickOilPaintImage(mt, threshold);
	} 
	else if ( ! strcmp(func, "MagickRandomThresholdImage" ) ) {
		double low = 0; double high = 0; 
		if ( sscanf(params, "%lf,%lf", &low, &high ) != 2 ){ 
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Function %s needs 2 paramters!", func); GoodbyeMessage(info, "Function %s needs 2 paramters!", func);	
			if(mt) DestroyMagickWand(mt); MagickWandTerminus(); return 400; 
		}
		MagickRandomThresholdImage(mt, low, high);
	} 
	else if ( ! strcmp(func, "MagickSepiaToneImage" ) ) {
		double threshold = 0; 
		if ( sscanf(params, "%lf", &threshold ) != 1 ){ 
			ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Function %s needs 1 paramters!", func); GoodbyeMessage(info, "Function %s needs 1 paramters!", func);	
			if(mt) DestroyMagickWand(mt); MagickWandTerminus(); return 400; 
		}
		MagickSepiaToneImage(mt, threshold);
	} 

	else { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to find %s filter!", func); GoodbyeMessage(info, "Unable to find %s filter!", func); if(mt) DestroyMagickWand(mt); MagickWandTerminus(); return 400; }	


	// Write
	blob = MagickGetImagesBlob(mt, &blobSize);

        if ( blob == NULL ) { ap_log_rerror(APLOG_MARK, APLOG_NOERRNO|APLOG_NOTICE, 0, r, "ERROR: Unable to create PNG with logo"); return 500; }
        if ( *realRaster != NULL ) free(*realRaster);

        *realRaster      = (GByte *)malloc(blobSize); memcpy(*realRaster, blob, blobSize );
        *pnOutDataLength = blobSize;


	// Clean
	if(mt)   mt   = DestroyMagickWand(mt);
	MagickWandTerminus();


	return 0;
}
