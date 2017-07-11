#requires -version 2
function nbnam([string]$mbr){
  process{
    if($_){$mbr=$_}
    $dom,$nam=gwmi win32_computersystem -computer $mbr|%{$_.domain,$_.name}
    $dn='dc={0}' -f $dom.replace('.',',dc=')
    $root=[adsi]"LDAP://$dom/rootdse"
    $cnc=$root.properties['configurationnamingcontext']
    $ds=[adsisearcher][adsi]"LDAP://$cnc"
    $ds.filter="(ncname=$dn)"
    [void]$ds.propertiestoload.add('netbiosname')
    @{dom=$ds.findone().properties['netbiosname'][0];nam=$nam}
  }
}
$sw=[diagnostics.stopwatch]::startnew()
cls
$path='\\s33rnp01\backups$\mssql'
$prin='s33rnp01'
$mirr='s33rnp10'
$move='D:\MSSQL\MSSQL10_50.MSSQLSERVER\MSSQL\DATA'
$prep=$true
$dbas=-split @'
LocalDB_33_AIB
LocalDB_33AVAFA
LocalDB_33AVBAG
LocalDB_33AVPET
LocalDB_33AVURT
LocalDB_33AVURT_LTS
LocalDB_33DABOR
LocalDB_33DAPOG
LocalDB_33GNSER
LocalDB_33IRVID
LocalDB_33IRVID_LTS
LocalDB_33KVZOB
LocalDB_33KVZOB_LTS
LocalDB_33LAFOM
LocalDB_33LAPON
LocalDB_33LIBOR
LocalDB_33LOCHE
LocalDB_33LPNOS
LocalDB_33MGTUR
LocalDB_33SGVJA
LocalDB_33SLFOK
LocalDB_33SVAGA
LocalDB_33SVAGA_LTS
LocalDB_33SVKAR
LocalDB_33SVMAZ
LocalDB_33SVSYS
LocalDB_33TPTEB
LocalDB_33VPNEG
LocalDB_33VVROS
LocalDB_SECURITY
LocalDB_XPRESS
ServerDB
SoftAdm
'@
$sb=new-object text.stringbuilder
try{
  $smo='Microsoft.SqlServer.Smo'
  [void][reflection.assembly]::loadwithpartialname($smo)
  [void][reflection.assembly]::loadwithpartialname("${smo}Extended")
  if($prin -notmatch '([a-z]{1}[\da-z]*)(?:\\[a-z]{1}[\da-z]*)?'){
    throw new-object applicationexception `
      "Имя основного сервера '$prin' не в формате 'сервер[\экземпляр]'"
  }
  if($prep){$hpri=[net.dns]::gethostentry($matches[1]).hostname}
  $sqlm=new-object microsoft.sqlserver.management.smo.server $mirr
  if($sqlm.netname){
    $hmir=[net.dns]::gethostentry($sqlm.netname).hostname
  }else{
    throw new-object applicationexception `
      "Сервер-зеркало '$mirr' недоступен"
  }
  $rest=new-object microsoft.sqlserver.management.smo.restore
  $bdev=new-object microsoft.sqlserver.management.smo.backupdeviceitem
  $bdev.devicetype='file'
  $rest.devices.add($bdev)
  $blis=@{}
  ls $path -r|?{!$_.psiscontainer}|%{
    $bdev.name=$fnam=$_.fullname
    try{$rows=$rest.readbackupheader($sqlm).rows}
    catch{
      [void]$sb.appendline('/*'
              ).appendline($fnam
              ).appendline(($_|out-string)
              ).appendline('*/')
      return
    }
    $rows|?{$_.servername -eq $prin}|
    ?{$dnam=$_.databasename;$dbas -contains $dnam}|
    ?{$_.backuptype -eq 1}|
    ?{'full',[dbnull]::value -contains $_.recoverymodel}|%{
      $brec=$blis[$dnam]
      $llsn=$_.lastlsn
      if(!$brec -or $brec.llsn -lt $llsn){
        if(!$brec){$blis[$dnam]=$brec=@{}}
        $brec.fnam=$fnam
        $brec.stim=$_.backupstartdate
        $brec.fnum=$fnum=$_.position
        $brec.llsn=$llsn
        if($move){
          $flis=$blis[$dnam].flis
          if($flis){$flis.clear()}else{$flis=$blis[$dnam].flis=@{}}
          $rest.filenumber=$fnum
          $rest.readfilelist($sqlm).rows|%{
            $lnam=$_.logicalname
            $pnam=$_.physicalname|split-path -leaf
            $flis[$lnam]=$pnam
          }
          $rest.filenumber=0
        }
      }
    }
  }
  if($blis.keys.count -gt 0){
    ls $path -r|?{!$_.psiscontainer}|%{
      $bdev.name=$fnam=$_.fullname
      try{$rows=$rest.readbackupheader($sqlm).rows}
      catch{
        [void]$sb.appendline('/*'
                ).appendline($fnam
                ).appendline(($_|out-string)
                ).appendline('*/')
        return
      }
      $rows|?{$_.servername -eq $prin}|
      ?{$dnam=$_.databasename;$blis.keys -contains $dnam}|
      ?{$_.backuptype -eq 2}|
      ?{'full',[dbnull]::value -contains $_.recoverymodel}|%{
        $brec=$blis[$dnam]
        $stim=$_.backupstartdate
        if($brec.stim -lt $stim){
          $flog=$brec.flog
          if(!$flog){$brec.flog=$flog=@{}}
          $flsn=$_.firstlsn
          $lkey="{0:$('0'*25)}{1:yyyyMMddHHmmssfff}" -f $flsn,$stim
          $flog[$lkey]=@{fnam=$fnam
                         fnum=$_.position
                         flsn=$flsn
                         llsn=$_.lastlsn
                         stim=$stim}
        }
      }
    }
    $blis.getenumerator()|%{
      $dnam=$_.key
      $brec=$_.value
      $llsn=$brec.llsn
      $flog=$brec.flog
      $lord=$null
      if($prep -and (!$flog -or ($flog[($flog.keys|sort)[0]]|
                                 ?{$llsn -lt $_.flsn -or
                                   $llsn -gt $_.llsn}))){
        throw new-object applicationexception `
          "Для БД '$dnam' нет резервных копиЙ журнала транзакций"
      }
      [void]$sb.appendline("restore database [$dnam] from"
              ).appendline("disk='$($brec.fnam)'"
              ).append("with file=$($brec.fnum),")
      if($move){
        $flis=$brec.flis.getenumerator()
        $movf="`nmove '{0}' to '{1}',"
        [void]$sb.appendline(
         -join ($flis|%{$movf -f $_.key,(join-path $move $_.value)})
        )
      }
      [void]$sb.append('replace,')
      if($flog){
        if(!$lord){$lord=$flog.getenumerator()|sort name}
        $lord|%{
          $lrec=$_.value
          if($llsn -eq $lrec.flsn -or ($llsn -gt $lrec.flsn -and
                                       $llsn -le $lrec.llsn)){
            [void]$sb.appendline('norecovery;'
                    ).appendline("restore log [$dnam] from"
                    ).appendline("disk='$($lrec.fnam)'"
                    ).append("with file=$($lrec.fnum),")
            $llsn=$lrec.llsn
          }else{return}
        }
      }
      if($prep){[void]$sb.append('no')}
      [void]$sb.appendline('recovery;')
    }
    [void]$sb.appendline('*/')
    if($prep){
      [void]$sb.appendline('/*'
              ).appendline("create endpoint [<name>] state=started as"
              ).appendline("  tcp(listener_port=<port>,listener_ip=all)"
              ).appendline('  for database_mirroring(role=partner);'
              ).appendline('alter authorization on endpoint::[<name>] to sa;'
              ).appendline('*/')
      $bkey=$blis.keys
      $setp="alter database [{0}] set partner='tcp://{1}:<port>';`n"
      $lpri,$lmir=$hpri,$hmir|nbnam|%{'[{0}\{1}$]' -f $_.dom,$_.nam}
      @{lgn=$lpri;hst=$hpri},
      @{lgn=$lmir;hst=$hmir}|%{
        $lgn,$hst=$_.lgn,$_.hst
        [void]$sb.appendline('/*'
                ).appendline("create login $lgn from windows;"
                ).appendline("grant connect on endpoint::[<name>] to $lgn;"
                ).appendline('*/'
                ).appendline('/*'
                ).append(-join ($bkey|%{$setp -f $_,$hst})
                ).appendline('*/')
      }
    }
    $sb=(new-object text.stringbuilder).appendline('/*'
                                      ).appendline('use master;'
                                      ).appendline('go'
                                      ).append($sb.tostring())
    [void]$sb.appendline('/*'
            ).appendline('Serial'
            ).appendline($sw.elapsed.tostring()
            ).append('*/')
  }
}catch{
  [void]$sb.appendline('/*'
          ).appendline(($_|out-string)
          ).append('*/')
}
write-host $sb.tostring()
