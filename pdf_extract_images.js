// pdf_extract_images.js — PDF 内嵌图片提取库
// 给 weave js 算子使用，依赖 __native__ 沙箱绑定（inflate / btoa / atob）
var pdfExtractImages = (function() {
  function findStreamObject(text, objNum) {
    var re = /(\d+)\s+(\d+)\s+obj/g, m;
    while ((m = re.exec(text)) !== null) {
      if (m[1] !== objNum.toString()) continue;
      var objStart = m.index, endIdx = text.indexOf('endobj', objStart);
      if (endIdx === -1) continue;
      var objStr = text.substring(objStart, endIdx + 6);
      var si = objStr.indexOf('stream\n') >= 0 ? objStr.indexOf('stream\n') : objStr.indexOf('stream\r\n');
      if (si === -1) continue;
      var ei = objStr.lastIndexOf('endstream');
      if (ei === -1) continue;
      var dictStr = objStr.substring(0, si);
      var bodyStart = si + 'stream\n'.length;
      if (objStr.substring(si + 6, si + 7) === '\r') bodyStart++;
      return { dict: dictStr, data: objStr.substring(bodyStart, ei) };
    }
    return null;
  }
  function parseDict(d) {
    var w = d.match(/\/Width\s+(\d+)/), h = d.match(/\/Height\s+(\d+)/);
    var f = /\/DCTDecode/.test(d) ? 'DCTDecode' : /\/FlateDecode/.test(d) ? 'FlateDecode' : 'none';
    return { width: w ? parseInt(w[1]) : 0, height: h ? parseInt(h[1]) : 0, filter: f };
  }
  function str2bytes(s) { for (var i=0,a=[]; i<s.length; i++) a[i]=s.charCodeAt(i)&0xFF; return a; }
  function bytes2str(a) { for (var i=0,s=''; i<a.length; i++) s+=String.fromCharCode(a[i]); return s; }
  function extractBytes(obj) {
    var raw = obj.data;
    if (raw.length>0 && raw.charCodeAt(raw.length-1)===10) raw=raw.substring(0,raw.length-1);
    if (raw.length>0 && raw.charCodeAt(raw.length-1)===13) raw=raw.substring(0,raw.length-1);
    var bytes = str2bytes(raw);
    if (/\/DCTDecode/.test(obj.dict)) return bytes;
    if (/\/FlateDecode/.test(obj.dict)) try { return __native__.inflate(bytes); } catch(e) { return null; }
    return bytes;
  }
  function extractImages(data_base64) {
    if (!data_base64) return [];
    var raw = __native__.atob(data_base64), text = bytes2str(raw), pages = [], pm;
    var pageRe = /\/Type\s*\/Page[^s]/g;
    while ((pm = pageRe.exec(text)) !== null) {
      var chunk = text.substring(pm.index, pm.index + 8000);
      var mb = chunk.match(/\/MediaBox\s*\[\s*([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s*\]/);
      var pw=0, ph=0;
      if (mb) { pw = Math.round(parseFloat(mb[3])-parseFloat(mb[1])); ph = Math.round(parseFloat(mb[4])-parseFloat(mb[2])); }
      var res = chunk.match(/\/Resources\s*<<([\s\S]*?)>>/);
      if (!res) continue;
      var xobj = res[1].match(/\/XObject\s*<<([\s\S]*?)>>/);
      if (!xobj) continue;
      var imRe = /\/(Im\d*)\s+(\d+\s+\d+\s+R)/g, im;
      while ((im = imRe.exec(xobj[1])) !== null) {
        var refParts = im[2].trim().split(/\s+/);
        var so = findStreamObject(text, refParts[0]);
        if (!so) continue;
        var imgBytes = extractBytes(so);
        if (imgBytes) {
          var info = parseDict(so.dict);
          pages.push({ page: pages.length+1, width: info.width||pw, height: info.height||ph, filter: info.filter, base64: __native__.btoa(imgBytes) });
        }
      }
    }
    return pages;
  }
  return extractImages;
})();
