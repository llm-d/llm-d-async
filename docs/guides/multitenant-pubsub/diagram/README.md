# Animated architecture diagram

A slick, looping diagram of the multi-tenant Pub/Sub demo: messages flow per-team
through subscriptions → worker pools → dispatch gates → the inference gateway → vLLM,
and the loop tells the **priority-under-saturation** story — as vLLM saturates, the
**batch** gate closes and its backlog grows while **premium** keeps flowing.

## Files

| File | Use |
| :-- | :-- |
| `architecture.html` | The source — self-contained animated SVG (CSS keyframes + SMIL). Open in a browser; animates live. Edit this. |
| `architecture.gif` | Looping GIF for embedding in a README (GitHub strips inline-SVG animation, so use the GIF). |
| `architecture.mp4` | Smaller/sharper version for slides or a web page. |
| `capture.sh` | Regenerates the GIF/MP4 from the HTML. |

## View it live

```bash
open diagram/architecture.html      # or: xdg-open / just open the file in a browser
```

## Embed in a README

```markdown
![Async Processor multi-tenant demo](diagram/architecture.gif)
```

## Regenerate the GIF/MP4

Requires `google-chrome` (headless) and `ffmpeg`:

```bash
./diagram/capture.sh
```

It scrubs Chrome's virtual clock one frame at a time (`--virtual-time-budget`, which
advances both CSS and SMIL animations), screenshots each frame, and assembles a
palette-optimized GIF + an H.264 MP4 of one 12s loop.

## Tweaking

Everything is driven by one shared loop period (`--loop` in the `<style>` block); all
timed effects (gate open/close, saturation meter, backlog bar, dot flow) use keyframe
percentages of that period, so they stay in sync. Colors are CSS variables at the top.
