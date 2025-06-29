# voicecheck.py  – drop-in patch
import os, tempfile, numpy as np
import ffmpeg, librosa, torch
from panns_inference import AudioTagging

SR      = 32000
THRESH  = 0.30
DEVICE  = "cuda" if torch.cuda.is_available() else "cpu"

AT_MODEL = AudioTagging(checkpoint_path=None, device=DEVICE)

def _extract_wav(video_fp: str, wav_fp: str, sr: int = SR) -> None:
    ffmpeg.input(video_fp).output(wav_fp, ac=1, ar=sr, format="wav")\
          .overwrite_output().run(quiet=True)

def _to_numpy(t):
    """Accept torch.Tensor | np.ndarray and return np.ndarray"""
    if isinstance(t, np.ndarray):
        return t
    if torch.is_tensor(t):
        return t.detach().cpu().numpy()
    raise TypeError(type(t))

def classify(video_fp: str) -> str:
    """
    Return one of:
      'lyrics' | 'instr_vo' | 'vo_only' | 'instrumental' | 'silence'
    """
    with tempfile.TemporaryDirectory() as td:
        wav_fp = os.path.join(td, "tmp.wav")
        _extract_wav(video_fp, wav_fp)
        y, _ = librosa.load(wav_fp, sr=SR, mono=True)

        tensor = torch.from_numpy(y[None, :])        # shape (1, samples)
        clipwise, _ = AT_MODEL.inference(tensor)      # (1, n_classes) *or* ndarray
        scores = _to_numpy(clipwise)[0]               # ndarray (527,)

    labels = AT_MODEL.labels
    idx = {lab: i for i, lab in enumerate(labels)}

    speech   = float(scores[idx.get("Speech",   -1)]) if "Speech"   in idx else 0.0
    music    = float(scores[idx.get("Music",    -1)]) if "Music"    in idx else 0.0
    singing  = float(scores[idx.get("Singing",  -1)]) if "Singing"  in idx else 0.0
    rap      = float(scores[idx.get("Rap",      -1)]) if "Rap"      in idx else 0.0

    if max(singing, rap) >= THRESH:
        return "lyrics"
    if speech >= THRESH and music >= THRESH:
        return "instr_vo"
    if speech >= THRESH:
        return "vo_only"
    if music >= THRESH:
        return "instrumental"
    return "silence"