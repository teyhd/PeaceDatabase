import matplotlib.pyplot as plt
import matplotlib.patches as patches

fig, ax = plt.subplots(figsize=(10,6))

boxes = [
    ("Input Sequence", 0.1, 0.75, 0.8, 0.12),
    ("Positional Encoding", 0.32, 0.62, 0.36, 0.1),
    ("Multi-Head Self-Attention", 0.27, 0.47, 0.46, 0.12),
    ("Add & Norm", 0.4, 0.37, 0.2, 0.07),
    ("Feed-Forward Network", 0.33, 0.24, 0.34, 0.1),
    ("Add & Norm", 0.4, 0.15, 0.2, 0.07),
    ("Pooling", 0.43, 0.05, 0.14, 0.07),
    ("Classifier Output", 0.38, -0.05, 0.24, 0.1),
]

for text, x, y, w, h in boxes:
    ax.add_patch(patches.Rectangle((x,y), w, h, fill=False, linewidth=2))
    ax.text(x+w/2, y+h/2, text, ha='center', va='center', fontsize=10)

# drawing arrows
arrow_positions = [(0.5,0.75,0.5,0.62),
                   (0.5,0.62,0.5,0.47),
                   (0.5,0.47,0.5,0.37),
                   (0.5,0.37,0.5,0.24),
                   (0.5,0.24,0.5,0.15),
                   (0.5,0.15,0.5,0.05),
                   (0.5,0.05,0.5,-0.05)]

for x1,y1,x2,y2 in arrow_positions:
    ax.annotate("", xy=(x2,y2), xytext=(x1,y1),
                 arrowprops=dict(arrowstyle="->", lw=2))

ax.set_xlim(0,1)
ax.set_ylim(-0.15,1)
ax.axis('off')

plt.tight_layout()
plt.show()
