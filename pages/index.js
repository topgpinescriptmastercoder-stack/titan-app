import { useEffect, useState } from 'react';
import Head from 'next/head';

export default function Home() {
  const [telegramId, setTelegramId] = useState('UNKNOWN');

  useEffect(() => {
    if (typeof window !== "undefined" && window.Telegram && window.Telegram.WebApp) {
      const tg = window.Telegram.WebApp;
      tg.ready();
      if (tg.initDataUnsafe && tg.initDataUnsafe.user) {
        setTelegramId(tg.initDataUnsafe.user.id);
      }
    }
  }, []);

  // UPDATE THE TWO LINKS BELOW. DO NOT DELETE THE ?client_reference_id PART.
  const starterLink = `prod_U96KaZGY9svkbpclient_reference_id=${telegramId}`;
  const proLink = `prod_U96QWc9BFScnSZclient_reference_id=${telegramId}`;

  return (
    <div style={{ padding: '20px', fontFamily: 'Arial, sans-serif', backgroundColor: '#000', color: '#fff', minHeight: '100vh' }}>
      <Head>
        <script src="https://telegram.org/js/telegram-web-app.js" async></script>
      </Head>
      
      <h1 style={{ textAlign: 'center', color: '#f39c12' }}>TITAN PRO</h1>
      <p style={{ textAlign: 'center', color: '#888' }}>ID: {telegramId}</p>

      <div style={{ marginTop: '30px', border: '1px solid #333', padding: '20px', borderRadius: '8px' }}>
        <h2>Starter Tier ($99)</h2>
        <p style={{ color: '#aaa' }}>1-2 high-quality signals daily. VIP Access.</p>
        <a href={starterLink} style={{ display: 'block', backgroundColor: '#f39c12', color: '#000', padding: '15px', textAlign: 'center', borderRadius: '5px', textDecoration: 'none', fontWeight: 'bold' }}>UPGRADE TO STARTER</a>
      </div>

      <div style={{ marginTop: '20px', border: '1px solid #333', padding: '20px', borderRadius: '8px' }}>
        <h2>Pro Tier ($199)</h2>
        <p style={{ color: '#aaa' }}>2-4 premium signals. Capital guidance.</p>
        <a href={proLink} style={{ display: 'block', backgroundColor: '#fff', color: '#000', padding: '15px', textAlign: 'center', borderRadius: '5px', textDecoration: 'none', fontWeight: 'bold' }}>UPGRADE TO PRO</a>
      </div>
    </div>
  );
}

