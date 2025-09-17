import React from 'react';

interface DatabricksLogoProps {
  height?: number;
  width?: number;
  className?: string;
}

const DatabricksLogo: React.FC<DatabricksLogoProps> = ({ 
  height = 32, 
  width = 32, 
  className = '' 
}) => {
  return (
    <img
      src="/databricks-logo.svg"
      alt="Databricks"
      width={width}
      height={height}
      className={className}
      style={{
        objectFit: 'contain',
        display: 'block'
      }}
    />
  );
};

export default DatabricksLogo;
