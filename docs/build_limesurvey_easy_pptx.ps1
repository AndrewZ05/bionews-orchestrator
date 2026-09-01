param(
    [string]$Source = "docs/LimeSurvey_Pipeline_v2.pptx",
    [string]$Destination = "docs/LimeSurvey_ETL_Easy_Overview.pptx"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Save-Utf8NoBom {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Content
    )

    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Content, $utf8NoBom)
}

function Set-SelectedSlides {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PresentationPath,
        [Parameter(Mandatory = $true)]
        [array]$Slides
    )

    [xml]$presentation = Get-Content -LiteralPath $PresentationPath -Raw
    $ns = New-Object System.Xml.XmlNamespaceManager($presentation.NameTable)
    $ns.AddNamespace("p", "http://schemas.openxmlformats.org/presentationml/2006/main")
    $ns.AddNamespace("r", "http://schemas.openxmlformats.org/officeDocument/2006/relationships")

    $sldIdLst = $presentation.SelectSingleNode("//p:sldIdLst", $ns)
    while ($sldIdLst.HasChildNodes) {
        $null = $sldIdLst.RemoveChild($sldIdLst.FirstChild)
    }

    foreach ($slide in $Slides) {
        $node = $presentation.CreateElement("p", "sldId", $ns.LookupNamespace("p"))

        $idAttr = $presentation.CreateAttribute("id")
        $idAttr.Value = [string]$slide.Id
        $null = $node.Attributes.Append($idAttr)

        $ridAttr = $presentation.CreateAttribute("r", "id", $ns.LookupNamespace("r"))
        $ridAttr.Value = [string]$slide.RelId
        $null = $node.Attributes.Append($ridAttr)

        $null = $sldIdLst.AppendChild($node)
    }

    $presentation.Save($PresentationPath)
}

function Update-AppProperties {
    param(
        [Parameter(Mandatory = $true)]
        [string]$AppPropsPath,
        [Parameter(Mandatory = $true)]
        [int]$SlideCount
    )

    [xml]$app = Get-Content -LiteralPath $AppPropsPath -Raw
    $ns = New-Object System.Xml.XmlNamespaceManager($app.NameTable)
    $ns.AddNamespace("ep", "http://schemas.openxmlformats.org/officeDocument/2006/extended-properties")
    $ns.AddNamespace("vt", "http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes")

    $slidesNode = $app.SelectSingleNode("/ep:Properties/ep:Slides", $ns)
    $slidesNode.InnerText = [string]$SlideCount

    $headingPairSlideCount = $app.SelectSingleNode("/ep:Properties/ep:HeadingPairs/vt:vector/vt:variant[6]/vt:i4", $ns)
    $headingPairSlideCount.InnerText = [string]$SlideCount

    $titlesVector = $app.SelectSingleNode("/ep:Properties/ep:TitlesOfParts/vt:vector", $ns)
    while ($titlesVector.ChildNodes.Count -gt 3) {
        $null = $titlesVector.RemoveChild($titlesVector.LastChild)
    }
    1..$SlideCount | ForEach-Object {
        $node = $app.CreateElement("vt", "lpstr", $ns.LookupNamespace("vt"))
        $node.InnerText = "PowerPoint Presentation"
        $null = $titlesVector.AppendChild($node)
    }
    $titlesVector.SetAttribute("size", [string](3 + $SlideCount))

    $app.Save($AppPropsPath)
}

function Ensure-PngContentType {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ContentTypesPath
    )

    [xml]$contentTypes = Get-Content -LiteralPath $ContentTypesPath -Raw
    $ns = New-Object System.Xml.XmlNamespaceManager($contentTypes.NameTable)
    $ns.AddNamespace("ct", "http://schemas.openxmlformats.org/package/2006/content-types")

    $pngNode = $contentTypes.SelectSingleNode("/ct:Types/ct:Default[@Extension='png']", $ns)
    if (-not $pngNode) {
        $typesNode = $contentTypes.SelectSingleNode("/ct:Types", $ns)
        $newNode = $contentTypes.CreateElement("Default", $ns.LookupNamespace("ct"))
        $newNode.SetAttribute("Extension", "png")
        $newNode.SetAttribute("ContentType", "image/png")
        $null = $typesNode.PrependChild($newNode)
        $contentTypes.Save($ContentTypesPath)
    }
}

function Write-UsageSlide {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SlidePath,
        [Parameter(Mandatory = $true)]
        [string]$SlideRelsPath
    )

    $slideXml = @"
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<p:sld xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main">
  <p:cSld>
    <p:spTree>
      <p:nvGrpSpPr>
        <p:cNvPr id="1" name=""/>
        <p:cNvGrpSpPr/>
        <p:nvPr/>
      </p:nvGrpSpPr>
      <p:grpSpPr>
        <a:xfrm>
          <a:off x="0" y="0"/>
          <a:ext cx="0" cy="0"/>
          <a:chOff x="0" y="0"/>
          <a:chExt cx="0" cy="0"/>
        </a:xfrm>
      </p:grpSpPr>
      <p:sp>
        <p:nvSpPr>
          <p:cNvPr id="2" name="Title Bar"/>
          <p:cNvSpPr/>
          <p:nvPr/>
        </p:nvSpPr>
        <p:spPr>
          <a:xfrm>
            <a:off x="0" y="0"/>
            <a:ext cx="12192000" cy="914400"/>
          </a:xfrm>
          <a:prstGeom prst="rect"><a:avLst/></a:prstGeom>
          <a:solidFill><a:srgbClr val="1A73E8"/></a:solidFill>
          <a:ln><a:noFill/></a:ln>
        </p:spPr>
        <p:txBody>
          <a:bodyPr rtlCol="0" anchor="ctr"/>
          <a:lstStyle/>
          <a:p><a:pPr algn="ctr"/><a:endParaRPr/></a:p>
        </p:txBody>
      </p:sp>
      <p:sp>
        <p:nvSpPr>
          <p:cNvPr id="3" name="Title"/>
          <p:cNvSpPr txBox="1"/>
          <p:nvPr/>
        </p:nvSpPr>
        <p:spPr>
          <a:xfrm>
            <a:off x="457200" y="201168"/>
            <a:ext cx="11277295" cy="548640"/>
          </a:xfrm>
          <a:prstGeom prst="rect"><a:avLst/></a:prstGeom>
          <a:noFill/>
        </p:spPr>
        <p:txBody>
          <a:bodyPr wrap="square"><a:spAutoFit/></a:bodyPr>
          <a:lstStyle/>
          <a:p>
            <a:pPr algn="l">
              <a:defRPr sz="3000" b="1">
                <a:solidFill><a:srgbClr val="FFFFFF"/></a:solidFill>
              </a:defRPr>
            </a:pPr>
            <a:r><a:t>How It Gets Used</a:t></a:r>
          </a:p>
        </p:txBody>
      </p:sp>
      <p:sp>
        <p:nvSpPr>
          <p:cNvPr id="4" name="Subtitle"/>
          <p:cNvSpPr txBox="1"/>
          <p:nvPr/>
        </p:nvSpPr>
        <p:spPr>
          <a:xfrm>
            <a:off x="457200" y="1051560"/>
            <a:ext cx="11277295" cy="365760"/>
          </a:xfrm>
          <a:prstGeom prst="rect"><a:avLst/></a:prstGeom>
          <a:noFill/>
        </p:spPr>
        <p:txBody>
          <a:bodyPr wrap="square"><a:spAutoFit/></a:bodyPr>
          <a:lstStyle/>
          <a:p>
            <a:pPr algn="l">
              <a:defRPr sz="1600">
                <a:solidFill><a:srgbClr val="666666"/></a:solidFill>
              </a:defRPr>
            </a:pPr>
            <a:r><a:t>Clean survey data moves straight into real analysis and identity-linked use cases.</a:t></a:r>
          </a:p>
        </p:txBody>
      </p:sp>
      <p:pic>
        <p:nvPicPr>
          <p:cNvPr id="5" name="Usage Graphic" descr="LimeSurvey ETL usage graphic"/>
          <p:cNvPicPr><a:picLocks noChangeAspect="1"/></p:cNvPicPr>
          <p:nvPr/>
        </p:nvPicPr>
        <p:blipFill>
          <a:blip r:embed="rId2"/>
          <a:stretch><a:fillRect/></a:stretch>
        </p:blipFill>
        <p:spPr>
          <a:xfrm>
            <a:off x="457200" y="1554480"/>
            <a:ext cx="7315200" cy="4876800"/>
          </a:xfrm>
          <a:prstGeom prst="rect"><a:avLst/></a:prstGeom>
          <a:ln>
            <a:solidFill><a:srgbClr val="D2E3FC"/></a:solidFill>
          </a:ln>
        </p:spPr>
      </p:pic>
      <p:sp>
        <p:nvSpPr>
          <p:cNvPr id="6" name="Callout Box"/>
          <p:cNvSpPr/>
          <p:nvPr/>
        </p:nvSpPr>
        <p:spPr>
          <a:xfrm>
            <a:off x="8229600" y="1554480"/>
            <a:ext cx="3505200" cy="4876800"/>
          </a:xfrm>
          <a:prstGeom prst="roundRect"><a:avLst/></a:prstGeom>
          <a:solidFill><a:srgbClr val="E8F0FE"/></a:solidFill>
          <a:ln>
            <a:solidFill><a:srgbClr val="1A73E8"/></a:solidFill>
          </a:ln>
        </p:spPr>
        <p:txBody>
          <a:bodyPr rtlCol="0" anchor="ctr"/>
          <a:lstStyle/>
          <a:p><a:pPr algn="ctr"/><a:endParaRPr/></a:p>
        </p:txBody>
      </p:sp>
      <p:sp>
        <p:nvSpPr>
          <p:cNvPr id="7" name="Callout Title"/>
          <p:cNvSpPr txBox="1"/>
          <p:nvPr/>
        </p:nvSpPr>
        <p:spPr>
          <a:xfrm>
            <a:off x="8458200" y="1783080"/>
            <a:ext cx="3048000" cy="365760"/>
          </a:xfrm>
          <a:prstGeom prst="rect"><a:avLst/></a:prstGeom>
          <a:noFill/>
        </p:spPr>
        <p:txBody>
          <a:bodyPr wrap="square"><a:spAutoFit/></a:bodyPr>
          <a:lstStyle/>
          <a:p>
            <a:pPr algn="l">
              <a:defRPr sz="2000" b="1">
                <a:solidFill><a:srgbClr val="1A73E8"/></a:solidFill>
              </a:defRPr>
            </a:pPr>
            <a:r><a:t>Used for</a:t></a:r>
          </a:p>
        </p:txBody>
      </p:sp>
      <p:sp>
        <p:nvSpPr>
          <p:cNvPr id="8" name="Callout Bullets"/>
          <p:cNvSpPr txBox="1"/>
          <p:nvPr/>
        </p:nvSpPr>
        <p:spPr>
          <a:xfrm>
            <a:off x="8458200" y="2240280"/>
            <a:ext cx="3048000" cy="2194560"/>
          </a:xfrm>
          <a:prstGeom prst="rect"><a:avLst/></a:prstGeom>
          <a:noFill/>
        </p:spPr>
        <p:txBody>
          <a:bodyPr wrap="square"><a:spAutoFit/></a:bodyPr>
          <a:lstStyle/>
          <a:p>
            <a:pPr>
              <a:spcAft><a:spcPts val="500"/></a:spcAft>
              <a:defRPr sz="1600">
                <a:solidFill><a:srgbClr val="333333"/></a:solidFill>
              </a:defRPr>
            </a:pPr>
            <a:r><a:t>- Dashboards and reporting</a:t></a:r>
          </a:p>
          <a:p>
            <a:pPr>
              <a:spcAft><a:spcPts val="500"/></a:spcAft>
              <a:defRPr sz="1600">
                <a:solidFill><a:srgbClr val="333333"/></a:solidFill>
              </a:defRPr>
            </a:pPr>
            <a:r><a:t>- Self-service analysis</a:t></a:r>
          </a:p>
          <a:p>
            <a:pPr>
              <a:spcAft><a:spcPts val="500"/></a:spcAft>
              <a:defRPr sz="1600">
                <a:solidFill><a:srgbClr val="333333"/></a:solidFill>
              </a:defRPr>
            </a:pPr>
            <a:r><a:t>- Identity-linked profiles</a:t></a:r>
          </a:p>
          <a:p>
            <a:pPr>
              <a:spcAft><a:spcPts val="500"/></a:spcAft>
              <a:defRPr sz="1600">
                <a:solidFill><a:srgbClr val="333333"/></a:solidFill>
              </a:defRPr>
            </a:pPr>
            <a:r><a:t>- Audience segmentation</a:t></a:r>
          </a:p>
          <a:p>
            <a:pPr>
              <a:spcAft><a:spcPts val="500"/></a:spcAft>
              <a:defRPr sz="1600">
                <a:solidFill><a:srgbClr val="333333"/></a:solidFill>
              </a:defRPr>
            </a:pPr>
            <a:r><a:t>- Better patient insight</a:t></a:r>
          </a:p>
        </p:txBody>
      </p:sp>
      <p:sp>
        <p:nvSpPr>
          <p:cNvPr id="9" name="Callout Footer"/>
          <p:cNvSpPr txBox="1"/>
          <p:nvPr/>
        </p:nvSpPr>
        <p:spPr>
          <a:xfrm>
            <a:off x="8458200" y="5303520"/>
            <a:ext cx="3048000" cy="640080"/>
          </a:xfrm>
          <a:prstGeom prst="rect"><a:avLst/></a:prstGeom>
          <a:noFill/>
        </p:spPr>
        <p:txBody>
          <a:bodyPr wrap="square"><a:spAutoFit/></a:bodyPr>
          <a:lstStyle/>
          <a:p>
            <a:pPr algn="l">
              <a:defRPr sz="1300">
                <a:solidFill><a:srgbClr val="666666"/></a:solidFill>
              </a:defRPr>
            </a:pPr>
            <a:r><a:t>One ETL makes survey answers usable across teams.</a:t></a:r>
          </a:p>
        </p:txBody>
      </p:sp>
    </p:spTree>
  </p:cSld>
  <p:clrMapOvr><a:masterClrMapping/></p:clrMapOvr>
</p:sld>
"@

    $relsXml = @"
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideLayout" Target="../slideLayouts/slideLayout7.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" Target="../media/limesurvey_etl_usage.png"/>
</Relationships>
"@

    Save-Utf8NoBom -Path $SlidePath -Content $slideXml
    Save-Utf8NoBom -Path $SlideRelsPath -Content $relsXml
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$sourcePath = Join-Path $repoRoot $Source
$destinationPath = Join-Path $repoRoot $Destination
$usageImagePath = Join-Path $repoRoot "docs\assets\limesurvey_etl_usage.png"

if (-not (Test-Path -LiteralPath $sourcePath)) {
    throw "Source PPTX not found: $sourcePath"
}
if (-not (Test-Path -LiteralPath $usageImagePath)) {
    throw "Usage image not found: $usageImagePath"
}

$buildRoot = Join-Path $repoRoot "temp\limesurvey_etl_easy_build"
$buildZip = Join-Path $repoRoot "temp\limesurvey_etl_easy_build.zip"
$outputZip = Join-Path $repoRoot "temp\limesurvey_etl_easy_output.zip"

Remove-Item -LiteralPath $buildRoot -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $buildZip -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $outputZip -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Path $buildRoot | Out-Null

Copy-Item -LiteralPath $sourcePath -Destination $buildZip -Force
Expand-Archive -LiteralPath $buildZip -DestinationPath $buildRoot -Force

$selectedSlides = @(
    @{ Id = 256; RelId = "rId2"  },  # slide1
    @{ Id = 257; RelId = "rId3"  },  # slide2
    @{ Id = 258; RelId = "rId4"  },  # slide3
    @{ Id = 260; RelId = "rId6"  },  # slide5
    @{ Id = 261; RelId = "rId7"  },  # slide6
    @{ Id = 262; RelId = "rId8"  },  # slide7
    @{ Id = 265; RelId = "rId9"  },  # slide8
    @{ Id = 271; RelId = "rId13" },  # slide12 (custom usage slide)
    @{ Id = 266; RelId = "rId17" },  # slide16
    @{ Id = 269; RelId = "rId12" }   # slide11
)

Set-SelectedSlides -PresentationPath (Join-Path $buildRoot "ppt\presentation.xml") -Slides $selectedSlides
Update-AppProperties -AppPropsPath (Join-Path $buildRoot "docProps\app.xml") -SlideCount $selectedSlides.Count
Ensure-PngContentType -ContentTypesPath (Join-Path $buildRoot "[Content_Types].xml")

$slide11Path = Join-Path $buildRoot "ppt\slides\slide11.xml"
$slide11Xml = Get-Content -LiteralPath $slide11Path -Raw
$slide11Xml = $slide11Xml.Replace("<a:t>Summary</a:t>", "<a:t>Key Benefits</a:t>")
$slide11Xml = $slide11Xml.Replace("<a:t>One pipeline, many payoffs</a:t>", "<a:t>Why teams care</a:t>")
Save-Utf8NoBom -Path $slide11Path -Content $slide11Xml

$mediaDir = Join-Path $buildRoot "ppt\media"
New-Item -ItemType Directory -Path $mediaDir -Force | Out-Null
Copy-Item -LiteralPath $usageImagePath -Destination (Join-Path $mediaDir "limesurvey_etl_usage.png") -Force

Write-UsageSlide `
    -SlidePath (Join-Path $buildRoot "ppt\slides\slide12.xml") `
    -SlideRelsPath (Join-Path $buildRoot "ppt\slides\_rels\slide12.xml.rels")

Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::CreateFromDirectory($buildRoot, $outputZip)
Copy-Item -LiteralPath $outputZip -Destination $destinationPath -Force

Write-Output "Created $destinationPath"
