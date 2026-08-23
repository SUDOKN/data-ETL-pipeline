"""Search-stage self-evaluation WITHOUT the pipeline's prompts (user ask, 2026-08-22, finding 8 / Q3).
Four products windows of run 20260822T223715 (two per subject) were read in full by Claude (Fable 5) and every search
form was judged by the plain meaning of the field — products ∪ contract products = goods the manufacturer makes or offers
(own lines/series/part types, parts made to order). Codes per form, in the order of `products_eval_lists.txt`:
  V valid product designation     B borderline (component, option, elliptical type, third-party part offered, family)
  P process / service / finish    M material                  G generic noun (parts, components, prototypes)
  C the CLIENT's product / application (not the manufacturer's)   S spec / attribute / document / equipment / facility / feature
  U website / UI junk or fragment
Recall = my own list of product designations from the text, each with the search form that covers it (None = missed).
"""
import os, re
HERE=os.path.dirname(os.path.abspath(__file__))
lists=open(f"{HERE}/products_eval_lists.txt").read().split("\n##### ")[1:]
windows={}
for sec in lists:
    head,*rows=sec.split("\n")
    forms=[re.match(r"\s*- (.*?)\s+<<", r).group(1) for r in rows if r.strip().startswith("- ")]
    windows[head.rsplit(":",1)[0]]=forms
codes={
 "alecmfg.com products window 0:24296":
   "PPPPPPGGGGGGGGGBMMMMMMMMMMMMMMMMMMMMMMSSGGGUGGSGSSSMMMMMMUSSPSSSPSSGSPPPPPSUSSSSVV",
 "alecmfg.com products window 98612:123329":
   "PGMSSSSSCS"+"SSBCBPPSSB"+"SSSSSSSGCB"+"PPCCPCCCCV"+"SSPPSSSBVB"+"CMMSSSPSSS"+"SBBSSSSSSS"+"MSSVSPSSSS"+"SSPSSPPSPS"+"SSSSGSBCVB"+"GVCVVGCVCV"+"CPCVCSUSSS"+"SUMPPPPGSB"+"CSSSSSSSPP"+"PPPPSPPPPP"+"SGSS",
 "steelcraft.com products window 0:23757":
   "VVVBVBBVVB"+"BBVBBBUVVB"+"VVVVVVVVVV"+"VVBVVVBVBB"+"BBVVVVGVVV"+"VVBBBBBVVV"+"VVVVSSSSBS"+"SSSSBBBSVV"+"VVVBBVMBBB"+"BBBSSSSSBS"+"B",
 "steelcraft.com products window 120661:144569":
   "VBSBSSSVVV"+"SSSSVBBBBB"+"VVVVBSSUCV"+"VVVVVSBSCC"+"GVVVVSSVSP"+"SVVSVBVBSS"+"SBSVVVVVVV"+"BVSSSSSSBB"+"BSSSV",
}
recall={
 "alecmfg.com products window 0:24296":[
   ("High-Precision Stainless Steel Valve Body for Semiconductor Equipment","High-Precision Stainless Steel Valve Body for Semiconductor Equipment"),
   ("precision parts (about page)",None),("metal parts and assemblies","metal parts"),("plastic parts","plastic parts"),("prototypes","prototypes"),
   ("production parts","production parts"),("custom parts","custom parts"),("cylindrical parts","cylindrical parts"),("molds (mold design)","mold")],
 "alecmfg.com products window 98612:123329":[
   ("copper heat sink","precision-machined copper heat sink"),("thermal component prototype(s)","Thermal Component Prototype"),
   ("welded structural steel frames","Welded structural steel frames"),("structural frames for offshore skid systems","Welded structural steel frames"),
   ("machined interfaces","machined interfaces"),("aluminum alloy components","three aluminum alloy components"),
   ("titanium alloy component(s)","High-Precision Titanium Alloy Component"),("titanium components","titanium components"),
   ("aluminum parts","High-Precision Aluminum Parts"),("stainless steel valve body","High-Precision Stainless Steel Valve Body"),
   ("aluminum alloy sheet metal structural housing","Aluminum Alloy Sheet Metal Structural Housing"),("battery module housings","battery module housings"),
   ("precision sensor ring","Precision Sensor Ring"),("optical sensor ring","optical sensor ring"),("aluminum mounting brackets","aluminum mounting brackets"),
   ("lightweight CNC components","Lightweight CNC Components"),("precision-machined components","precision-machined components")],
 "steelcraft.com products window 0:23757":[
   ("hollow metal doors and frames","Steelcraft hollow metal doors and frames"),("steel doors and frames","steel doors and frames"),
   ("Paladin PW Series flush doors and frames","Paladin™ PW Series flush doors and frames"),("Paladin Series glass light steel tornado doors","Paladin™ Series Glass light steel tornado doors"),
   ("tornado door","tornado door"),("fire-rated tornado glass light","fire-rated tornado glass light"),("glass lights","glass lights"),
   ("Pilkington Pyrostop® glass",None),("T Series Flush Doors","T Series Flush Doors"),("SL Series Square Edge Flush Doors","SL Series Square Edge Flush Doors"),
   ("LS Series Stainless Steel Doors","LS Series Stainless Steel Doors"),("Hollow Metal Steel Flush Doors | L Series","Hollow Metal Steel Flush Doors"),
   ("A14 Series Entrance Doors","A14 Series Entrance Doors"),("SZ Series Falcon Flush Doors","SZ Series Falcon Flush Doors"),
   ("subcategories (Embossed … Temperature Rise)","Embossed"),("hurricane certified doors and frames","hurricane certified doors and frames"),
   ("H Series Hurricane Rated Flush Doors","H Series Hurricane Rated Flush Doors"),("L Series Flush Doors","L Series Flush Doors"),
   ("P Series Tornado Safe Frames","P Series Tornado Safe Frames"),("flush / stile and rail / severe weather / acoustical / stainless steel / blast resistant doors","blast resistant doors"),
   ("GRAINTECH Series stainable steel doors","GRAINTECH™ Series of stainable steel doors"),("T Series temperature rise core doors","T Series temperature rise core doors"),
   ("H Series hurricane doors / TH Series","TH Series"),("special stairwell doors","Special stairwell doors"),("Hurricane Assemblies","Hurricane Assemblies"),
   ("tornado assembly components (frame, door, hinges, anchors, latching hardware)","tornado assembly components"),
   ("core options (honeycomb, polystyrene, polyurethane, mineral board, steel stiffened)","Honeycomb"),("Dezigner Glass Trim","Recessed Dezigner™ Glass Trim"),
   ("top and bottom caps","top and bottom caps"),("Schlage/Von Duprin hardware","Schlage® or Von Duprin® hardware")],
 "steelcraft.com products window 120661:144569":[
   ("C & CK Series frames","C & CK Series frames"),("stock doors and frames","stock doors and frames"),("custom frames",None),
   ("hollow metal doors and frames","hollow metal doors and frames"),("door types list","blast resistant doors"),
   ("GRAINTECH Series stainable steel doors","GRAINTECH™ Series of stainable steel doors"),("GRAINTECH doors","GRAINTECH doors"),
   ("CE Series panel doors","CE Series panel doors"),("L Series flush doors","L Series flush doors"),("T Series temperature rise doors","T Series temperature rise doors"),
   ("H Series hurricane doors","H Series hurricane doors"),("GRAINTECH finishes","GRAINTECH finishes"),("Dezigner trim flush lite kits","Dezigner™ trim flush lite kits"),
   ("CE/H16/HE16/L16/T16 constructions","H16 and HE16 Series"),("fire-rated doors and frames","fire-rated doors and frames"),("fire doors","Fire doors"),
   ("ceramic glass",None),("special glazing compounds",None),("FireLite fire-rated glass","FireLite® fire-rated glass"),
   ("fire-resistance-rated frames","fire-resistance-rated frames"),("fire-resistance-rated glazing","fire-resistance-rated glazing"),
   ("temperature rise door","temperature rise door"),("DE Series double egress frames","DE Series double egress frames"),("FE Series double egress frames","FE Series double egress frames"),
   ("swing clear hinges","swing clear hinges"),("field-installed silencers","field-installed silencers"),("custom profiles","custom profiles"),
   ("standard/heavyweight hinges","standard weight .134\" (3.3mm) thick hinges")],
}
from collections import Counter
tot=Counter(); print(f"{'window':48} {'n':>4}  V    B    P    M    G    C    S    U   | V+B  (V+B+G)")
for w,forms in windows.items():
    c=codes[w]; assert len(c)==len(forms), (w,len(c),len(forms))
    cnt=Counter(c); tot.update(cnt)
    vb=cnt['V']+cnt['B']; print(f"{w:48} {len(forms):4} "+" ".join(f"{cnt[k]:4}" for k in "VBPMGCSU")+f"  | {vb/len(forms):4.0%} ({(vb+cnt['G'])/len(forms):.0%})")
    # the individual judgments, for audit
    open(f"{HERE}/search_self_eval_judgments.txt","a").write(f"\n##### {w}\n"+"\n".join(f"  {k}  {f}" for f,k in zip(forms,c)))
n=sum(tot.values()); print(f"{'ALL 4 WINDOWS':48} {n:4} "+" ".join(f"{tot[k]:4}" for k in "VBPMGCSU")+f"  | {(tot['V']+tot['B'])/n:4.0%} ({(tot['V']+tot['B']+tot['G'])/n:.0%})")
a=sum(Counter(codes[w]).get(k,0) for w in codes if w.startswith('alecmfg') for k in 'VB'); an=sum(len(codes[w]) for w in codes if w.startswith('alecmfg'))
s=sum(Counter(codes[w]).get(k,0) for w in codes if w.startswith('steelcraft') for k in 'VB'); sn=sum(len(codes[w]) for w in codes if w.startswith('steelcraft'))
print(f"precision (V+B): alecmfg {a}/{an} = {a/an:.0%};  steelcraft {s}/{sn} = {s/sn:.0%}")
print("\nRECALL of my own product designations:")
rt=rf=0
for w,items in recall.items():
    f=sum(1 for _,m in items if m); rt+=len(items); rf+=f
    print(f"  {w:48} {f}/{len(items)}  missed: {[x for x,m in items if not m]}")
print(f"  TOTAL {rf}/{rt} = {rf/rt:.0%}")
