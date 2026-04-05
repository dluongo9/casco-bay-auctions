"""
Classify auction lot titles into categories/subcategories using Claude API.
Reads auction_test.csv, writes auction_test_classified.csv.

Usage:
    export ANTHROPIC_API_KEY=sk-...
    python3 classify.py
"""

import csv
import json
import os
import time

import anthropic

TAXONOMY = """
Categories and subcategories to use. Pick exactly one category and one subcategory per lot.

FURNITURE
  - Seating (chairs, settees, sofas, rockers, benches, stools, back stools)
  - Tables (dining, side, tea, card, drop-leaf, tilt-top, library, pembroke, tuckaway)
  - Case Furniture (chests, dressers, desks, cabinets, cupboards, hutches, coffres, bookcases)
  - Mirrors
  - Beds & Cradles
  - Stands & Shelves (plant stands, wall shelves, book stands, butler's tray stands)
  - Other Furniture (fire screens, easels, library steps, frames that hold things)

FINE ART - PAINTINGS
  - Portraits (oils, pastels of people)
  - Landscapes (oils, gouache of outdoor scenes)
  - Still Life (oils, gouache of objects, flowers, food)
  - Genre & Narrative (everyday life scenes, drinking parties, bar scenes)
  - Marine & Nautical Paintings (boats, harbors, sea)
  - Folk Art Paintings (naive, untrained, primitive style)

WORKS ON PAPER
  - Prints & Engravings (etchings, engravings, woodcuts, woodblocks, aquatints, mezzotints)
  - Lithographs (stone-printed, chromo)
  - Watercolors & Drawings (watercolor, gouache on paper, pencil, ink drawings, sketches)
  - Maps & Charts (geographic, nautical, topographic maps)
  - Natural History Illustrations (birds, animals, botanical prints - Audubon, Keulemans, Manetti)
  - Photographs & Daguerreotypes (photos, tintypes, daguerreotypes, ambrotypes)

SCULPTURE & THREE-DIMENSIONAL ART
  - Bronze Sculpture (cast bronze figures, reliefs, plaques)
  - Carved Wood Figures (santos, folk figures, animals)
  - Ceramics Sculpture (terracotta, pottery figures)
  - Other Sculpture

SILVER & JEWELRY
  - Sterling Silver (flatware, holloware, serving pieces)
  - Coin Silver (American coin silver)
  - Silverplate (EPNS, Sheffield)
  - Jewelry (rings, bracelets, necklaces, brooches)
  - Watches (pocket watches, wristwatches)

METALWARE
  - Brass (candlesticks, boxes, plates, buckets, alms plates, tobacco boxes)
  - Copper (pots, pans, jugs, molds)
  - Pewter (coffee pots, pitchers, urns, tankards)
  - Wrought Iron (tools, andirons, latches, roasters)
  - Cast Iron (doorstops, embossers, lighthouse lamps)
  - Bronze Objects (non-sculpture: bells, mortars, caskets)

CERAMICS & GLASS
  - Delft (Dutch and English tin-glazed earthenware)
  - Staffordshire (transfer-printed, lustreware, pearlware)
  - Chinese Export Porcelain (Canton, Rose Medallion, Famille Rose, blue & white)
  - European Porcelain (Herend, Wedgwood, Limoges, Coalport, Paris, Villeroy & Boch)
  - American Pottery (redware, stoneware, yellowware, Roseville)
  - Glass & Crystal (blown glass, pressed glass, Steuben, Waterford, crystal)
  - Other Ceramics

ASIAN ART & ANTIQUES
  - Chinese Decorative Arts (lacquer, brass, carved objects, screens, furniture)
  - Chinese Porcelain (vases, jars, floor vases — not export)
  - Japanese Art (woodblocks, scrolls, watercolors, tanto, ceramics)
  - South & Southeast Asian (Indian, Indonesian, Burmese, Thai objects)
  - Asian Textiles & Embroidery

TEXTILES & RUGS
  - Oriental Carpets & Rugs (Persian, Turkish, Caucasian, Chinese)
  - Hooked Rugs
  - Quilts & Coverlets
  - Samplers & Needlework
  - Flags & Pennants
  - Other Textiles (toile, silk, embroidery, lace)

DECORATIVE ARTS & OBJECTS
  - Clocks & Barometers
  - Lamps & Lighting (oil lamps, sconces, candelabra, chandeliers, astral lamps)
  - Boxes & Small Objects (snuff boxes, patch boxes, desk boxes, tea caddies, inkwells)
  - Tole & Painted Tinware (painted toleware trays, canisters, coffee pots, spice boxes)
  - Frames (picture frames, gilt, ebonized, carved)
  - Globes
  - Scientific & Navigational Instruments
  - Taxidermy & Natural History Objects

FOLK ART & AMERICANA
  - Decoys & Carved Birds
  - Fraktur & Pennsylvania German (watercolor fraktur, decorated documents)
  - Scrimshaw
  - Tramp Art
  - Folk Carvings & Sculptures (other carved folk objects)
  - Fraternal & Civic Objects (Odd Fellows, medals, badges)
  - Americana & Advertising (signs, jars, political, sporting memorabilia)

BOOKS, DOCUMENTS & EPHEMERA
  - Books & Periodicals
  - Maps & Atlases (also appears under Works on Paper — use this for loose maps)
  - Documents, Deeds & Manuscripts
  - Advertising & Ephemera (signs, calendars, trade cards)
  - Auction Catalogs

MARITIME & NAUTICAL
  - Ship Models
  - Navigational Instruments (logs, wheels, compasses)
  - Maritime Decorative (lighthouse items, whale art, nautical charts)

MILITARY & ARMS
  - Civil War (weapons, documents, daguerreotypes, memorabilia)
  - Other Weapons (swords, daggers)
  - Military Memorabilia

TOYS, DOLLS & CHILDHOOD
  - Dolls & Doll Accessories
  - Toys & Games (tin toys, mechanical, cast iron, board games)
  - Stuffed Animals & Soft Toys
  - Children's Furniture & Accessories

AFRICAN & TRIBAL ART
  - Masks
  - Baskets & Woven Objects
  - Figures & Carvings

NATIVE AMERICAN ART
  - Beadwork
  - Other

CLOTHING & ACCESSORIES
  - Garments (capes, jackets, uniforms)
  - Footwear
  - Bags & Cases (satchels, pocketbooks)
  - Sewing & Dressmaking Tools

KITCHEN & DOMESTIC
  - Cookware & Food Preparation
  - Flatware & Cutlery
  - Serving Pieces
  - Sewing Machines & Tools

MUSICAL INSTRUMENTS
  - Wind Instruments
  - Stringed Instruments
  - Other Instruments

SPORTS & LEISURE
  - Sports Memorabilia
  - Games & Recreation

UNCATEGORIZED
  - Unknown
"""

SYSTEM_PROMPT = f"""You are an expert auction cataloger specializing in antiques, fine art, and decorative arts.

Your job is to classify auction lot titles into exactly one category and subcategory from the taxonomy below.

{TAXONOMY}

Rules:
- Return ONLY a valid JSON array, no explanation, no markdown.
- Each element: {{"n": <lot_number_string>, "cat": "<category>", "sub": "<subcategory>"}}
- Use the exact category and subcategory names from the taxonomy.
- If genuinely unclear, use "UNCATEGORIZED" / "Unknown".
"""


def classify_batch(client, batch):
    """batch: list of (lot_number, title) tuples. Returns list of dicts."""
    lines = "\n".join(f'{n}. [{lot_num}] {title}' for n, (lot_num, title) in enumerate(batch, 1))

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        messages=[
            {
                "role": "user",
                "content": f"Classify these auction lot titles:\n\n{lines}\n\nReturn JSON array only."
            }
        ],
        system=SYSTEM_PROMPT,
    )

    raw = message.content[0].text.strip()
    # Strip markdown code fences if present
    raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    return json.loads(raw)


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set.")
        return

    client = anthropic.Anthropic(api_key=api_key)

    infile = "auction_test.csv"
    outfile = "auction_test_classified.csv"

    with open(infile, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"Classifying {len(rows)} lots using Claude API...")

    # Build lookup by lot_number
    results = {}  # lot_number -> (category, subcategory)

    batch_size = 50
    batches = []
    for i in range(0, len(rows), batch_size):
        chunk = [(r["lot_number"], r["lot_title"]) for r in rows[i:i+batch_size]]
        batches.append(chunk)

    for i, batch in enumerate(batches):
        print(f"  Batch {i+1}/{len(batches)} ({len(batch)} lots)...", end=" ", flush=True)
        try:
            classifications = classify_batch(client, batch)
            for item in classifications:
                results[str(item["n"])] = (item["cat"], item["sub"])
            print(f"done ({len(classifications)} classified)")
        except Exception as e:
            print(f"ERROR: {e}")
            # Leave unclassified items as Uncategorized
            for lot_num, _ in batch:
                results[lot_num] = ("UNCATEGORIZED", "Unknown")
        time.sleep(0.2)

    # Write output
    fieldnames = list(rows[0].keys()) + ["category", "subcategory"]
    cat_counts = {}

    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            lot_num = row["lot_number"]
            cat, sub = results.get(lot_num, ("UNCATEGORIZED", "Unknown"))
            row["category"] = cat
            row["subcategory"] = sub
            key = f"{cat} > {sub}"
            cat_counts[key] = cat_counts.get(key, 0) + 1
            writer.writerow(row)

    print(f"\nSaved to {outfile}")
    print("\nCategory breakdown:")
    for key, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
        print(f"  {count:4d}  {key}")


if __name__ == "__main__":
    main()
