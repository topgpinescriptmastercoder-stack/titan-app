import Head from 'next/head';
import { useEffect, useState } from 'react';

export default function Home() {
  const [uid, setUid] = useState('');
  const [ready, setReady] = useState(false);

  useEffect(() => {
    const tg = window?.Telegram?.WebApp;
    if (tg) {
      tg.ready();
      tg.expand();
      const id = tg.initDataUnsafe?.user?.id;
      setUid(id ? String(id) : 'MEMBER');
    } else {
      setUid('MEMBER');
    }
    setReady(true);
  }, []);

  const base = uid || 'MEMBER';
  const starter = `https://buy.stripe.com/28EdR85iZ835brF2Ot4Ni01-starter?client_reference_id=${base}`;
  const pro = `https://buy.stripe.com/3cI14mfXD6Z1fHV4WB4Ni02-pro?client_reference_id=${base}`;

  if (!ready) return (
    <div style={{background:'#0a0a0a',minHeight:'100vh',
      display:'flex',alignItems:'center',justifyContent:'center'}}>
      <div style={{color:'#f39c12',fontFamily:'monospace',
        fontSize:13,letterSpacing:3}}>INITIALIZING...</div>
    </div>
  );

  return (
    <>
      <Head>
        <title>TITAN PRO</title>
        <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1"/>
        <script src="https://telegram.org/js/telegram-web-app.js"/>
        <link href="https://fonts.googleapis.com/css2?family=Rajdhani:wght@600;700&family=IBM+Plex+Mono&display=swap" rel="stylesheet"/>
      </Head>

      <div style={{background:'#0a0a0a',minHeight:'100vh',color:'#fff',
        fontFamily:"'IBM Plex Mono',monospace",maxWidth:440,margin:'0 auto',padding:20}}>

        {/* HEADER */}
        <div style={{textAlign:'center',paddingBottom:20,borderBottom:'1px solid #1a1a1a'}}>
          <div style={{fontFamily:'Rajdhani',fontSize:28,fontWeight:700,
            color:'#f39c12',letterSpacing:4}}>TITAN PRO</div>
          <div style={{fontSize:9,color:'#333',letterSpacing:2,marginTop:4}}>
            ARANYA GENESIS CORP
          </div>
          <div style={{fontSize:10,color:'#444',marginTop:8}}>
            UID: <span style={{color:'#f39c12'}}>{uid}</span>
          </div>
        </div>

        {/* LIVE GOLD PRICE STRIP */}
        <div style={{margin:'16px 0',padding:'10px 14px',
          background:'#111',border:'1px solid #1e1e1e',borderRadius:8,
          display:'flex',justifyContent:'space-between',alignItems:'center'}}>
          <span style={{fontSize:10,color:'#555',letterSpacing:2}}>XAUUSD</span>
          <span style={{fontSize:10,color:'#f39c12',letterSpacing:1}}>
            SIGNALS LIVE
          </span>
          <span style={{fontSize:9,color:'#333'}}>CA ENGINE ACTIVE</span>
        </div>

        {/* STARTER */}
        <div style={{border:'1px solid #2a2000',borderRadius:10,
          background:'linear-gradient(135deg,#0f0f00,#1a1200)',
          padding:20,marginBottom:16}}>
          <div style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
            <div>
              <div style={{fontFamily:'Rajdhani',fontSize:20,fontWeight:700}}>
                STARTER
              </div>
              <div style={{fontSize:9,color:'#666',marginTop:2}}>MONTHLY</div>
            </div>
            <div style={{color:'#f39c12',fontSize:28,fontFamily:'Rajdhani',fontWeight:700}}>
              $99
            </div>
          </div>
          <div style={{margin:'12px 0',borderTop:'1px solid #2a2000'}}/>
          {['1–2 XAUUSD signals daily',
            'VVIP Telegram access',
            'Entry · SL · TP1/TP2/TP3',
            'Weekly macro report'].map(f=>(
            <div key={f} style={{fontSize:11,color:'#888',marginBottom:6,display:'flex',gap:8}}>
              <span style={{color:'#f39c12'}}>▸</span>{f}
            </div>
          ))}
          <a href={starter} style={{display:'block',marginTop:16,
            background:'#f39c12',color:'#000',padding:14,textAlign:'center',
            borderRadius:6,textDecoration:'none',fontWeight:700,
            fontSize:12,letterSpacing:2,fontFamily:'Rajdhani'}}>
            ACTIVATE STARTER →
          </a>
        </div>

        {/* PRO */}
        <div style={{border:'1px solid #002a15',borderRadius:10,
          background:'linear-gradient(135deg,#000f08,#001a0f)',
          padding:20,position:'relative'}}>
          <div style={{position:'absolute',top:-10,right:16,
            background:'#00ff88',color:'#000',fontSize:9,fontWeight:700,
            padding:'3px 10px',borderRadius:20,letterSpacing:2}}>
            RECOMMENDED
          </div>
          <div style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
            <div>
              <div style={{fontFamily:'Rajdhani',fontSize:20,fontWeight:700}}>PRO</div>
              <div style={{fontSize:9,color:'#666',marginTop:2}}>MONTHLY</div>
            </div>
            <div style={{color:'#00ff88',fontSize:28,fontFamily:'Rajdhani',fontWeight:700}}>
              $199
            </div>
          </div>
          <div style={{margin:'12px 0',borderTop:'1px solid #002a15'}}/>
          {['2–4 premium signals daily',
            'Priority VVIP access',
            'Capital sizing guidance',
            'Real-time SL modification alerts',
            'Direct support line'].map(f=>(
            <div key={f} style={{fontSize:11,color:'#888',marginBottom:6,display:'flex',gap:8}}>
              <span style={{color:'#00ff88'}}>▸</span>{f}
            </div>
          ))}
          <a href={pro} style={{display:'block',marginTop:16,
            background:'#00ff88',color:'#000',padding:14,textAlign:'center',
            borderRadius:6,textDecoration:'none',fontWeight:700,
            fontSize:12,letterSpacing:2,fontFamily:'Rajdhani'}}>
            ACTIVATE PRO →
          </a>
        </div>

        <p style={{textAlign:'center',color:'#1a1a1a',fontSize:9,marginTop:24,letterSpacing:1}}>
          TITAN PRO · aranyagenesis.xyz · ARANYA GENESIS CORP
        </p>
      </div>
    </>
  );
}
