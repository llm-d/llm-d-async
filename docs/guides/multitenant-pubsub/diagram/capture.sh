#!/usr/bin/env bash
# Regenerate architecture.gif / architecture.mp4 from architecture.html.
# Requires: google-chrome (headless) and ffmpeg.
#
# Captures one full 12s loop by advancing Chrome's virtual clock per frame
# (--virtual-time-budget advances CSS *and* SMIL animations before each
# screenshot), then assembles the frames with ffmpeg.
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="file://$DIR/architecture.html"
FRAMES="$(mktemp -d)"
LOOP_MS=12000; FPS=10; STEP=$((1000/FPS)); i=0

for ((t=0; t<LOOP_MS; t+=STEP)); do
  b=$t; [ "$b" -lt 50 ] && b=50
  google-chrome --headless=new --disable-gpu --no-sandbox --hide-scrollbars \
    --window-size=1280,760 --force-device-scale-factor=1 \
    --virtual-time-budget="$b" --screenshot="$FRAMES/$(printf '%03d' "$i").png" "$SRC" >/dev/null 2>&1 || true
  i=$((i+1))
done

ffmpeg -y -framerate "$FPS" -i "$FRAMES/%03d.png" \
  -vf "scale=900:-1:flags=lanczos,split[s0][s1];[s0]palettegen=max_colors=128[p];[s1][p]paletteuse=dither=bayer" \
  -loop 0 "$DIR/architecture.gif"

ffmpeg -y -framerate "$FPS" -i "$FRAMES/%03d.png" -c:v libx264 -pix_fmt yuv420p \
  -vf "scale=1280:-2" -movflags +faststart "$DIR/architecture.mp4"

rm -rf "$FRAMES"
echo "wrote $DIR/architecture.gif and architecture.mp4"
