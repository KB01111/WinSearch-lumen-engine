"""Generate isolated native acceptance PDFs (bundled Python: Pillow/reportlab)."""
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from reportlab.lib.pdfencrypt import StandardEncryption

root = Path(__file__).resolve().parents[1] / "artifacts" / "pdf-fixtures"
root.mkdir(parents=True, exist_ok=True)
c = canvas.Canvas(str(root / "fyrtio-Å界.pdf"), pageCompression=0)
for page in range(1, 41):
    c.drawString(50, 700, f"PDF_MARKER_PAGE_{page:02d} sharedpdfneedle")
    c.showPage()
c.save()
image = Image.new("RGB", (1600, 1000), "white")
draw = ImageDraw.Draw(image)
font = ImageFont.truetype("C:/Windows/Fonts/arial.ttf", 90)
draw.text((80, 380), "SCANNED INVOICE 42", font=font, fill="black")
c = canvas.Canvas(str(root / "scanned.pdf"), pagesize=(800, 500))
c.drawImage(ImageReader(image), 0, 0, 800, 500)
c.showPage()
c.save()
c = canvas.Canvas(str(root / "scanned-two.pdf"), pagesize=(800, 500))
for number in (42, 43):
    image = Image.new("RGB", (1600, 1000), "white")
    ImageDraw.Draw(image).text((80, 380), f"SCANNED INVOICE {number}", font=font, fill="black")
    c.drawImage(ImageReader(image), 0, 0, 800, 500); c.showPage()
c.save()
c = canvas.Canvas(str(root / "empty.pdf")); c.showPage(); c.save()
c = canvas.Canvas(str(root / "numeric.pdf")); c.drawString(50, 700, "98017 12000 44200 18900 50100"); c.showPage(); c.save()
(root / "corrupt.pdf").write_bytes(b"%PDF-1.7\ncorrupt")
c = canvas.Canvas(str(root / "protected.pdf"), encrypt=StandardEncryption("secret"))
c.drawString(50, 700, "protectedtext"); c.showPage(); c.save()
c = canvas.Canvas(str(root / "huge-text.pdf"), pagesize=(2000, 2000), pageCompression=0)
# A real text page large enough to exceed the 256-draft transport window.
text = c.beginText(10, 1900); text.setFont("Helvetica", 1); text.setLeading(3)
for line in range(300): text.textLine("a" * 1000)
text.textLine("finalhugepageneedle"); c.drawText(text)
c.showPage(); c.save()
c = canvas.Canvas(str(root / "pathological-size.pdf"), pagesize=(100000000, 1))
c.drawString(0, 0, "bounded"); c.showPage(); c.save()
c = canvas.Canvas(str(root / "pathological-text.pdf"), pagesize=(2000, 2000), pageCompression=0)
text = c.beginText(10, 1900); text.setFont("Helvetica", 1); text.setLeading(3)
for line in range(600): text.textLine("a" * 1000)
c.drawText(text); c.showPage(); c.save()
c = canvas.Canvas(str(root / "over-page-limit.pdf"))
for page in range(5001): c.showPage()
c.save()
c = canvas.Canvas(str(root / "later-page-error.pdf"))
for page in range(40):
    c.setPageSize((100000000, 1) if page == 34 else (612, 792))
    c.drawString(50, 700, f"retainedpagetoken{page+1}")
    c.showPage()
c.save()
print(root)
