"""
Media Converter - Convert recordings to MP4 and extract AAC audio
"""

import os
import subprocess
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class MediaConverter:
    """Convert media files using ffmpeg"""
    
    def __init__(self):
        """Initialize media converter"""
        # Verify ffmpeg is available
        # Use a simple which/command check instead of running ffmpeg -version
        # which can hang in some Docker environments
        try:
            # First, check if ffmpeg exists in PATH using 'which'
            result = subprocess.run(
                ['which', 'ffmpeg'],
                capture_output=True,
                text=True,
                timeout=2
            )
            if result.returncode == 0:
                logger.info(f"ffmpeg found at: {result.stdout.strip()}")
            else:
                logger.warning("ffmpeg not found in PATH")
                raise RuntimeError("ffmpeg is required but not available")
        except subprocess.TimeoutExpired:
            logger.error("Timeout while checking for ffmpeg")
            raise RuntimeError("ffmpeg check timed out")
        except FileNotFoundError:
            # 'which' command not available, try direct ffmpeg check with shorter timeout
            try:
                result = subprocess.run(
                    ['ffmpeg', '-version'],
                    capture_output=True,
                    text=True,
                    timeout=2,
                    stdin=subprocess.DEVNULL  # Prevent ffmpeg from waiting for input
                )
                if result.returncode == 0:
                    logger.info("ffmpeg is available")
                else:
                    logger.warning("ffmpeg check returned non-zero exit code")
                    raise RuntimeError("ffmpeg is required but not available")
            except Exception as e:
                logger.error(f"ffmpeg not found or not working: {e}")
                raise RuntimeError("ffmpeg is required but not available")
        except Exception as e:
            logger.error(f"Error checking for ffmpeg: {e}")
            raise RuntimeError("ffmpeg is required but not available")
    
    def convert(self, input_path: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Convert recording to MP4 and extract AAC audio
        
        Args:
            input_path: Path to the input recording file
            
        Returns:
            Tuple of (mp4_path, aac_path) if successful, (None, None) otherwise
        """
        if not os.path.exists(input_path):
            logger.error(f"Input file not found: {input_path}")
            return None, None
        
        # Generate output paths
        base_path = os.path.splitext(input_path)[0]
        mp4_path = f"{base_path}.mp4"
        aac_path = f"{base_path}.aac"
        
        # Convert to MP4
        mp4_success = self._convert_to_mp4(input_path, mp4_path)
        if not mp4_success:
            return None, None
        
        # Extract AAC audio
        aac_success = self._extract_aac(input_path, aac_path)
        if not aac_success:
            return None, None
        
        return mp4_path, aac_path
    
    def _convert_to_mp4(self, input_path: str, output_path: str) -> bool:
        """
        Convert video to MP4 format
        
        Args:
            input_path: Input video file
            output_path: Output MP4 file path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Converting to MP4: {input_path} -> {output_path}")
            
            # ffmpeg command for MP4 conversion
            # Using H.264 codec with ultra high quality settings
            # Audio filter chain for professional quality:
            # 1. highpass: Remove low frequency rumble/noise below 80Hz
            # 2. lowpass: Remove high frequency hiss above 15kHz
            # 3. afftdn: Advanced noise reduction using FFT
            # 4. loudnorm: Professional loudness normalization (EBU R128)
            audio_filters = (
                'highpass=f=80,'                          # Remove rumble
                'lowpass=f=15000,'                        # Remove hiss
                'afftdn=nf=-25,'                          # Noise reduction
                'loudnorm=I=-16:TP=-1.5:LRA=11'           # Normalize loudness
            )
            
            cmd = [
                'ffmpeg',
                '-i', input_path,
                '-c:v', 'libx264',      # Video codec
                '-preset', 'slow',       # Slower encoding for better quality
                '-crf', '18',            # Quality (lower = better, 18 is very high quality, was 23)
                '-af', audio_filters,    # Audio filter chain
                '-c:a', 'aac',           # Audio codec
                '-b:a', '384k',          # Audio bitrate (3x from 128k)
                '-ar', '48000',          # 48 kHz sample rate
                '-movflags', '+faststart',  # Enable streaming
                '-y',                    # Overwrite output file
                output_path
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout
                stdin=subprocess.DEVNULL  # Prevent ffmpeg from waiting for input
            )
            
            if result.returncode == 0:
                logger.info(f"Successfully converted to MP4: {output_path}")
                return True
            else:
                logger.error(f"MP4 conversion failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("MP4 conversion timed out")
            return False
        except Exception as e:
            logger.exception(f"Error during MP4 conversion: {e}")
            return False
    
    def _extract_aac(self, input_path: str, output_path: str) -> bool:
        """
        Extract audio as AAC
        
        Args:
            input_path: Input video file
            output_path: Output AAC file path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Extracting AAC audio: {input_path} -> {output_path}")
            
            # ffmpeg command for AAC extraction with advanced audio processing
            # Multi-stage audio filter chain for professional quality:
            # 1. highpass: Remove low frequency rumble/noise below 80Hz
            # 2. lowpass: Remove high frequency hiss above 15kHz
            # 3. afftdn: Advanced noise reduction using FFT
            # 4. loudnorm: Professional loudness normalization (EBU R128)
            # 5. compand: Dynamic range compression for clearer speech
            audio_filters = (
                'highpass=f=80,'                          # Remove rumble
                'lowpass=f=15000,'                        # Remove hiss
                'afftdn=nf=-25,'                          # Noise reduction
                'loudnorm=I=-16:TP=-1.5:LRA=11,'          # Normalize loudness
                'compand=attacks=0.3:decays=0.8:points=-80/-80|-45/-30|-27/-20|0/-10'  # Compress dynamics
            )
            
            cmd = [
                'ffmpeg',
                '-i', input_path,
                '-vn',                   # No video
                '-af', audio_filters,    # Audio filter chain
                '-c:a', 'aac',           # Audio codec
                '-b:a', '384k',          # Audio bitrate (3x from 128k)
                '-ar', '48000',          # 48 kHz sample rate
                '-y',                    # Overwrite output file
                output_path
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=1800,  # 30 minute timeout
                stdin=subprocess.DEVNULL  # Prevent ffmpeg from waiting for input
            )
            
            if result.returncode == 0:
                logger.info(f"Successfully extracted AAC audio: {output_path}")
                return True
            else:
                logger.error(f"AAC extraction failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("AAC extraction timed out")
            return False
        except Exception as e:
            logger.exception(f"Error during AAC extraction: {e}")
            return False
    
    def cleanup(self, *file_paths: str):
        """
        Clean up temporary files
        
        Args:
            *file_paths: File paths to delete
        """
        for file_path in file_paths:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"Cleaned up file: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to cleanup file {file_path}: {e}")
